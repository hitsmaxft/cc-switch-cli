use super::*;
use crate::{database::Database, proxy::circuit_breaker::CircuitBreakerConfig};
use serde_json::json;
use serial_test::serial;
use std::{env, sync::Arc};
use tempfile::TempDir;

struct TempHome {
    #[allow(dead_code)]
    dir: TempDir,
    original_home: Option<String>,
    original_userprofile: Option<String>,
}

impl TempHome {
    fn new() -> Self {
        let dir = TempDir::new().expect("failed to create temp home");
        let original_home = env::var("HOME").ok();
        let original_userprofile = env::var("USERPROFILE").ok();

        env::set_var("HOME", dir.path());
        env::set_var("USERPROFILE", dir.path());
        crate::settings::reload_test_settings();

        Self {
            dir,
            original_home,
            original_userprofile,
        }
    }
}

impl Drop for TempHome {
    fn drop(&mut self) {
        match &self.original_home {
            Some(value) => env::set_var("HOME", value),
            None => env::remove_var("HOME"),
        }

        match &self.original_userprofile {
            Some(value) => env::set_var("USERPROFILE", value),
            None => env::remove_var("USERPROFILE"),
        }

        crate::settings::reload_test_settings();
    }
}

#[tokio::test]
#[serial]
async fn test_provider_router_creation() {
    let _home = TempHome::new();
    let db = Arc::new(Database::memory().unwrap());
    let router = ProviderRouter::new(db);

    let breaker = router.get_or_create_circuit_breaker("claude:test").await;
    assert!(breaker.allow_request().await.allowed);
}

#[tokio::test]
#[serial]
async fn test_failover_disabled_uses_current_provider() {
    let _home = TempHome::new();
    let db = Arc::new(Database::memory().unwrap());

    let provider_a = Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
    let provider_b = Provider::with_id("b".to_string(), "Provider B".to_string(), json!({}), None);

    db.save_provider("claude", &provider_a).unwrap();
    db.save_provider("claude", &provider_b).unwrap();
    db.set_current_provider("claude", "a").unwrap();
    db.add_to_failover_queue("claude", "b").unwrap();

    let router = ProviderRouter::new(db.clone());
    let providers = router.select_providers("claude").await.unwrap();

    assert_eq!(providers.len(), 1);
    assert_eq!(providers[0].id, "a");
}

#[tokio::test]
#[serial]
async fn test_failover_disabled_uses_db_as_source_of_truth() {
    // The proxy is a long-running process; other processes (e.g. `cc-switch
    // provider switch`) only update the database.  The in-process settings
    // store (OnceLock) is never reloaded, so the DB must be the authority.
    let _home = TempHome::new();
    let db = Arc::new(Database::memory().unwrap());

    let provider_a = Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
    let provider_b = Provider::with_id("b".to_string(), "Provider B".to_string(), json!({}), None);

    db.save_provider("claude", &provider_a).unwrap();
    db.save_provider("claude", &provider_b).unwrap();

    // DB says "a"; in-process settings says "b" (simulates a stale cache).
    db.set_current_provider("claude", "a").unwrap();
    crate::settings::set_current_provider(&crate::app_config::AppType::Claude, Some("b")).unwrap();

    let router = ProviderRouter::new(db.clone());
    let providers = router.select_providers("claude").await.unwrap();

    // DB is the authority; the stale in-process value must be ignored.
    assert_eq!(providers.len(), 1);
    assert_eq!(providers[0].id, "a");
}

#[tokio::test]
#[serial]
async fn test_failover_enabled_uses_queue_order_ignoring_current() {
    let _home = TempHome::new();
    let db = Arc::new(Database::memory().unwrap());

    let mut provider_a =
        Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
    provider_a.sort_index = Some(2);
    let mut provider_b =
        Provider::with_id("b".to_string(), "Provider B".to_string(), json!({}), None);
    provider_b.sort_index = Some(1);

    db.save_provider("claude", &provider_a).unwrap();
    db.save_provider("claude", &provider_b).unwrap();
    db.set_current_provider("claude", "a").unwrap();
    db.add_to_failover_queue("claude", "b").unwrap();
    db.add_to_failover_queue("claude", "a").unwrap();

    let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
    config.auto_failover_enabled = true;
    db.update_proxy_config_for_app(config).await.unwrap();

    let router = ProviderRouter::new(db.clone());
    let providers = router.select_providers("claude").await.unwrap();

    assert_eq!(providers.len(), 2);
    assert_eq!(providers[0].id, "b");
    assert_eq!(providers[1].id, "a");
}

#[tokio::test]
#[serial]
async fn test_failover_enabled_without_queue_returns_no_providers_configured() {
    let _home = TempHome::new();
    let db = Arc::new(Database::memory().unwrap());

    let provider = Provider::with_id(
        "codex-current".to_string(),
        "Codex Current".to_string(),
        json!({}),
        None,
    );

    db.save_provider("codex", &provider).unwrap();
    db.set_current_provider("codex", "codex-current").unwrap();

    let mut config = db.get_proxy_config_for_app("codex").await.unwrap();
    config.auto_failover_enabled = true;
    db.update_proxy_config_for_app(config).await.unwrap();

    let router = ProviderRouter::new(db.clone());
    let error = router
        .select_providers("codex")
        .await
        .expect_err("empty failover queue should no longer fall back to current provider");

    assert!(matches!(error, ProxyError::NoProvidersConfigured));
}

#[tokio::test]
#[serial]
async fn test_select_providers_does_not_consume_half_open_permit() {
    let _home = TempHome::new();
    let db = Arc::new(Database::memory().unwrap());

    db.update_circuit_breaker_config(&CircuitBreakerConfig {
        failure_threshold: 1,
        timeout_seconds: 0,
        ..Default::default()
    })
    .await
    .unwrap();

    let provider_a = Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
    let provider_b = Provider::with_id("b".to_string(), "Provider B".to_string(), json!({}), None);

    db.save_provider("claude", &provider_a).unwrap();
    db.save_provider("claude", &provider_b).unwrap();
    db.add_to_failover_queue("claude", "a").unwrap();
    db.add_to_failover_queue("claude", "b").unwrap();

    let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
    config.auto_failover_enabled = true;
    db.update_proxy_config_for_app(config).await.unwrap();

    let router = ProviderRouter::new(db.clone());

    router
        .record_result("b", "claude", false, false, Some("fail".to_string()))
        .await
        .unwrap();

    let providers = router.select_providers("claude").await.unwrap();
    assert_eq!(providers.len(), 2);

    assert!(router.allow_provider_request("b", "claude").await.allowed);
}

#[tokio::test]
#[serial]
async fn test_release_permit_neutral_frees_half_open_slot() {
    let _home = TempHome::new();
    let db = Arc::new(Database::memory().unwrap());

    db.update_circuit_breaker_config(&CircuitBreakerConfig {
        failure_threshold: 1,
        timeout_seconds: 0,
        ..Default::default()
    })
    .await
    .unwrap();

    let provider_a = Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
    db.save_provider("claude", &provider_a).unwrap();
    db.add_to_failover_queue("claude", "a").unwrap();

    let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
    config.auto_failover_enabled = true;
    db.update_proxy_config_for_app(config).await.unwrap();

    let router = ProviderRouter::new(db.clone());

    router
        .record_result("a", "claude", false, false, Some("fail".to_string()))
        .await
        .unwrap();

    let first = router.allow_provider_request("a", "claude").await;
    assert!(first.allowed);
    assert!(first.used_half_open_permit);

    let second = router.allow_provider_request("a", "claude").await;
    assert!(!second.allowed);

    router
        .release_permit_neutral("a", "claude", first.used_half_open_permit)
        .await;

    let third = router.allow_provider_request("a", "claude").await;
    assert!(third.allowed);
    assert!(third.used_half_open_permit);
}

#[tokio::test]
#[serial]
async fn test_record_result_uses_app_failure_threshold_for_health_updates() {
    let _home = TempHome::new();
    let db = Arc::new(Database::memory().unwrap());

    let provider = Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
    db.save_provider("claude", &provider).unwrap();

    let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
    config.circuit_failure_threshold = 2;
    db.update_proxy_config_for_app(config).await.unwrap();

    let router = ProviderRouter::new(db.clone());

    router
        .record_result("a", "claude", false, false, Some("fail-1".to_string()))
        .await
        .unwrap();
    let first_health = db.get_provider_health("a", "claude").await.unwrap();
    assert!(first_health.is_healthy);
    assert_eq!(first_health.consecutive_failures, 1);

    router
        .record_result("a", "claude", false, false, Some("fail-2".to_string()))
        .await
        .unwrap();
    let second_health = db.get_provider_health("a", "claude").await.unwrap();
    assert!(!second_health.is_healthy);
    assert_eq!(second_health.consecutive_failures, 2);
    assert_eq!(second_health.last_error.as_deref(), Some("fail-2"));
}

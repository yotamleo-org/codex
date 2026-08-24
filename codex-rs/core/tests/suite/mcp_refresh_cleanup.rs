use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use codex_config::DEFAULT_MCP_SERVER_ENVIRONMENT_ID;
use codex_config::types::McpServerConfig;
use codex_config::types::McpServerTransportConfig;
use codex_core::NewThread;
use codex_core::config::Config;
use codex_core::StartThreadOptions;
use codex_core::TurnInputRequest;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::user_input::UserInput;
use core_test_support::process::process_is_alive;
use core_test_support::process::wait_for_pid_file;
use core_test_support::process::wait_for_process_exit;
use core_test_support::responses;
use core_test_support::skip_if_no_network;
use core_test_support::stdio_server_bin;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use core_test_support::wait_for_mcp_server;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_keeps_superseded_mcp_server_alive_for_in_flight_calls() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));

    let server = responses::start_mock_server().await;
    let temp_dir = tempfile::tempdir()?;
    let pid_file = temp_dir.path().join("mcp.pid");
    let pid_file_for_config = pid_file.clone();
    let command = stdio_server_bin()?;
    let fixture = test_codex()
        .with_config(move |config| {
            let mut servers = config.mcp_servers.get().clone();
            servers.insert(
                "refresh_cleanup".to_string(),
                McpServerConfig {
                    auth: Default::default(),
                    transport: McpServerTransportConfig::Stdio {
                        command,
                        args: Vec::new(),
                        env: Some(HashMap::from([(
                            "MCP_TEST_PID_FILE".to_string(),
                            pid_file_for_config.to_string_lossy().into_owned(),
                        )])),
                        env_vars: Vec::new(),
                        cwd: None,
                    },
                    environment_id: DEFAULT_MCP_SERVER_ENVIRONMENT_ID.to_string(),
                    enabled: true,
                    required: false,
                    supports_parallel_tool_calls: false,
                    omit_tools_from: None,
                    disabled_reason: None,
                    startup_timeout_sec: Some(Duration::from_secs(10)),
                    tool_timeout_sec: None,
                    default_tools_approval_mode: None,
                    enabled_tools: None,
                    disabled_tools: None,
                    scopes: None,
                    oauth: None,
                    oauth_resource: None,
                    tools: HashMap::new(),
                },
            );
            config
                .mcp_servers
                .set(servers)
                .expect("test MCP servers should accept any configuration");
        })
        .build(&server)
        .await?;
    wait_for_mcp_server(&fixture.codex, "refresh_cleanup").await?;

    let superseded_pid = wait_for_pid_file(&pid_file).await?;
    assert!(process_is_alive(&superseded_pid)?);

    let barrier = serde_json::json!({
        "id": "mcp-refresh-cleanup",
        "participants": 2,
        "timeout_ms": 1_000
    });
    let long_call = tokio::spawn({
        let codex = Arc::clone(&fixture.codex);
        let barrier = barrier.clone();
        async move {
            codex
                .call_mcp_tool(
                    "refresh_cleanup",
                    "sync",
                    Some(serde_json::json!({
                        "barrier": barrier,
                        "sleep_after_ms": 300_000
                    })),
                    /*meta*/ None,
                )
                .await
        }
    });
    fixture
        .codex
        .call_mcp_tool(
            "refresh_cleanup",
            "sync",
            Some(serde_json::json!({ "barrier": barrier })),
            /*meta*/ None,
        )
        .await?;
    fs::remove_file(&pid_file)?;

    responses::mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_response_created("resp-1"),
            responses::ev_assistant_message("msg-1", "done"),
            responses::ev_completed("resp-1"),
        ]),
    )
    .await;
    fixture.codex.submit(Op::RefreshMcpServers).await?;
    fixture.submit_turn("refresh MCP servers").await?;

    let replacement_pid = wait_for_pid_file(&pid_file).await?;
    assert_ne!(replacement_pid, superseded_pid);
    assert!(process_is_alive(&superseded_pid)?);
    long_call.abort();
    assert!(
        long_call
            .await
            .expect_err("call should be aborted")
            .is_cancelled()
    );
    wait_for_process_exit(&superseded_pid).await?;
    assert!(process_is_alive(&replacement_pid)?);

    fixture.codex.shutdown_and_wait().await?;
    wait_for_process_exit(&replacement_pid).await
}

const LIFECYCLE_SERVER_NAME: &str = "subagent_lifecycle";

/// Registers a stdio MCP server that records its own pid so tests can watch the process.
fn with_pid_reporting_stdio_server(config: &mut Config, pid_file: &Path) -> anyhow::Result<()> {
    let mut servers = config.mcp_servers.get().clone();
    servers.insert(
        LIFECYCLE_SERVER_NAME.to_string(),
        McpServerConfig {
            auth: Default::default(),
            transport: McpServerTransportConfig::Stdio {
                command: stdio_server_bin()?,
                args: Vec::new(),
                env: Some(HashMap::from([(
                    "MCP_TEST_PID_FILE".to_string(),
                    pid_file.to_string_lossy().into_owned(),
                )])),
                env_vars: Vec::new(),
                cwd: None,
            },
            environment_id: DEFAULT_MCP_SERVER_ENVIRONMENT_ID.to_string(),
            enabled: true,
            required: false,
            supports_parallel_tool_calls: false,
            omit_tools_from: None,
            disabled_reason: None,
            startup_timeout_sec: Some(Duration::from_secs(10)),
            tool_timeout_sec: None,
            default_tools_approval_mode: None,
            enabled_tools: None,
            disabled_tools: None,
            scopes: None,
            oauth: None,
            oauth_resource: None,
            tools: HashMap::new(),
        },
    );
    config
        .mcp_servers
        .set(servers)
        .expect("test MCP servers should accept any configuration");
    Ok(())
}

fn assistant_turn(prompt: &str) -> TurnInputRequest {
    TurnInputRequest::user_input(vec![UserInput::Text {
        text: prompt.to_string(),
        text_elements: Vec::new(),
    }])
}

/// A subagent thread stays resident after its turn, but must not keep its stdio fleet.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_subagent_releases_its_stdio_mcp_servers() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));

    let server = responses::start_mock_server().await;
    let temp_dir = tempfile::tempdir()?;
    let pid_file = temp_dir.path().join("subagent-mcp.pid");
    // The root thread configures no MCP servers, so the child below owns the only fleet.
    let fixture = test_codex().build(&server).await?;

    let mut subagent_config = fixture.config.clone();
    with_pid_reporting_stdio_server(&mut subagent_config, &pid_file)?;
    let NewThread {
        thread: subagent, ..
    } = fixture
        .thread_manager
        .start_thread(StartThreadOptions {
            session_source: Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: fixture.session_configured.thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
            ..StartThreadOptions::new(subagent_config)
        })
        .await?;
    wait_for_mcp_server(&subagent, LIFECYCLE_SERVER_NAME).await?;

    let subagent_pid = wait_for_pid_file(&pid_file).await?;
    assert!(process_is_alive(&subagent_pid)?);

    responses::mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_response_created("subagent-resp"),
            responses::ev_assistant_message("subagent-msg", "done"),
            responses::ev_completed("subagent-resp"),
        ]),
    )
    .await;
    subagent
        .start_or_steer_turn(assistant_turn("finish without using tools"))
        .await?;
    wait_for_event(&subagent, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;

    // The thread is still loaded; only its MCP fleet should be gone.
    wait_for_process_exit(&subagent_pid).await?;

    // The still-resident thread republishes a working fleet the next time it needs one.
    fs::remove_file(&pid_file)?;
    subagent
        .call_mcp_tool(
            LIFECYCLE_SERVER_NAME,
            "sync",
            Some(serde_json::json!({})),
            /*meta*/ None,
        )
        .await?;
    let resumed_pid = wait_for_pid_file(&pid_file).await?;
    assert_ne!(resumed_pid, subagent_pid);

    subagent.shutdown_and_wait().await?;
    fixture.codex.shutdown_and_wait().await?;
    Ok(())
}

/// Releasing the fleet must not cut off a tool call that is still running.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_subagent_keeps_in_flight_mcp_calls_alive() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));

    let server = responses::start_mock_server().await;
    let temp_dir = tempfile::tempdir()?;
    let pid_file = temp_dir.path().join("in-flight-mcp.pid");
    let fixture = test_codex().build(&server).await?;

    let mut subagent_config = fixture.config.clone();
    with_pid_reporting_stdio_server(&mut subagent_config, &pid_file)?;
    let NewThread {
        thread: subagent, ..
    } = fixture
        .thread_manager
        .start_thread(StartThreadOptions {
            session_source: Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: fixture.session_configured.thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
            ..StartThreadOptions::new(subagent_config)
        })
        .await?;
    wait_for_mcp_server(&subagent, LIFECYCLE_SERVER_NAME).await?;
    let subagent_pid = wait_for_pid_file(&pid_file).await?;

    // The barrier rendezvous below proves the long call is running before the turn ends.
    let barrier = serde_json::json!({
        "id": "subagent-release-in-flight",
        "participants": 2,
        "timeout_ms": 1_000
    });
    let long_call = tokio::spawn({
        let subagent = Arc::clone(&subagent);
        let barrier = barrier.clone();
        async move {
            subagent
                .call_mcp_tool(
                    LIFECYCLE_SERVER_NAME,
                    "sync",
                    Some(serde_json::json!({
                        "barrier": barrier,
                        "sleep_after_ms": 300_000
                    })),
                    /*meta*/ None,
                )
                .await
        }
    });
    subagent
        .call_mcp_tool(
            LIFECYCLE_SERVER_NAME,
            "sync",
            Some(serde_json::json!({ "barrier": barrier })),
            /*meta*/ None,
        )
        .await?;

    responses::mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_response_created("in-flight-resp"),
            responses::ev_assistant_message("in-flight-msg", "done"),
            responses::ev_completed("in-flight-resp"),
        ]),
    )
    .await;
    subagent
        .start_or_steer_turn(assistant_turn("finish while a tool call is running"))
        .await?;
    wait_for_event(&subagent, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;

    assert!(process_is_alive(&subagent_pid)?);
    long_call.abort();
    assert!(
        long_call
            .await
            .expect_err("call should be aborted")
            .is_cancelled()
    );
    wait_for_process_exit(&subagent_pid).await?;

    subagent.shutdown_and_wait().await?;
    fixture.codex.shutdown_and_wait().await?;
    Ok(())
}

/// The release above is scoped to subagents: a root thread keeps its fleet between turns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_root_turn_keeps_its_stdio_mcp_servers() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));

    let server = responses::start_mock_server().await;
    let temp_dir = tempfile::tempdir()?;
    let pid_file = temp_dir.path().join("root-mcp.pid");
    let pid_file_for_config = pid_file.clone();
    let fixture = test_codex()
        .with_config(move |config| {
            with_pid_reporting_stdio_server(config, &pid_file_for_config)
                .expect("test stdio server binary should be available");
        })
        .build(&server)
        .await?;
    wait_for_mcp_server(&fixture.codex, LIFECYCLE_SERVER_NAME).await?;

    let root_pid = wait_for_pid_file(&pid_file).await?;
    assert!(process_is_alive(&root_pid)?);

    responses::mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_response_created("root-resp"),
            responses::ev_assistant_message("root-msg", "done"),
            responses::ev_completed("root-resp"),
        ]),
    )
    .await;
    // submit_turn returns once the turn is complete.
    fixture.submit_turn("finish without using tools").await?;

    assert!(process_is_alive(&root_pid)?);

    fixture.codex.shutdown_and_wait().await?;
    wait_for_process_exit(&root_pid).await
}

//! Discord Gateway adapter for the OpenFang channel bridge.
//!
//! Uses Discord Gateway WebSocket (v10) for receiving messages and the REST API
//! for sending responses. No external Discord crate — just `tokio-tungstenite` + `reqwest`.
//!
//! Implements 5 phases:
//! 1. Rich Output (Embeds, Files, Images)
//! 2. Interactive Components (Buttons, Select Menus)
//! 3. Slash Commands
//! 4. Streaming (SSE live updates)
//! 5. Voice (Foundation only)

use crate::types::{
    split_message, ChannelAdapter, ChannelContent, ChannelMessage, ChannelType, ChannelUser,
    AgentPhase, default_phase_emoji, LifecycleReaction, RichBlock,
};
use async_trait::async_trait;
use dashmap::DashMap;
use futures::{SinkExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch, RwLock};
use tracing::{debug, error, info, warn};
use zeroize::Zeroizing;

const DISCORD_API_BASE: &str = "https://discord.com/api/v10";
const MAX_BACKOFF: Duration = Duration::from_secs(60);
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const DISCORD_MSG_LIMIT: usize = 2000;

/// Discord Gateway opcodes.
mod opcode {
    pub const DISPATCH: u64 = 0;
    pub const HEARTBEAT: u64 = 1;
    pub const IDENTIFY: u64 = 2;
    pub const RESUME: u64 = 6;
    pub const RECONNECT: u64 = 7;
    pub const INVALID_SESSION: u64 = 9;
    pub const HELLO: u64 = 10;
    pub const HEARTBEAT_ACK: u64 = 11;
}

/// Discord-specific types for rich content (Phase 1-2).
mod discord_types {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Default)]
    pub struct DiscordEmbed {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub title: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub description: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub color: Option<u32>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        pub fields: Vec<EmbedField>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub image: Option<EmbedMedia>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub thumbnail: Option<EmbedMedia>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub footer: Option<EmbedFooter>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub url: Option<String>,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct EmbedField {
        pub name: String,
        pub value: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub inline: Option<bool>,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct EmbedMedia {
        pub url: String,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct EmbedFooter {
        pub text: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub icon_url: Option<String>,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct ActionRow {
        #[serde(rename = "type")]
        pub component_type: u8, // always 1
        pub components: Vec<Component>,
    }

    #[derive(Debug, Clone, Serialize)]
    #[serde(untagged)]
    pub enum Component {
        Button {
            #[serde(rename = "type")]
            component_type: u8, // 2
            style: u8,          // 1=primary, 2=secondary, 3=success, 4=danger
            label: String,
            custom_id: String,
        },
        SelectMenu {
            #[serde(rename = "type")]
            component_type: u8, // 3
            custom_id: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            placeholder: Option<String>,
            options: Vec<SelectOption>,
            #[serde(skip_serializing_if = "Option::is_none")]
            min_values: Option<u32>,
            #[serde(skip_serializing_if = "Option::is_none")]
            max_values: Option<u32>,
        },
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct SelectOption {
        pub label: String,
        pub value: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub description: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub default: Option<bool>,
    }
}

/// Voice module (Phase 5 foundation).
mod voice {
    use songbird::{Config as SongbirdConfig, driver::{DecodeMode, Driver}};
    use songbird::events::{Event, EventContext, EventHandler as SongbirdEventHandler, CoreEvent};
    use songbird::id::{ChannelId, GuildId, UserId};
    use songbird::input::File as AudioFile;
    use songbird::ConnectionInfo;
    use std::collections::HashMap;
    use std::num::NonZeroU64;
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tracing::{debug, error, info, warn};

    pub mod voice_opcode {
        pub const IDENTIFY: u64 = 0;
        pub const SELECT_PROTOCOL: u64 = 1;
        pub const READY: u64 = 2;
        pub const HEARTBEAT: u64 = 3;
        pub const SESSION_DESCRIPTION: u64 = 4;
        pub const SPEAKING: u64 = 5;
        pub const HEARTBEAT_ACK: u64 = 6;
        pub const RESUME: u64 = 7;
        pub const HELLO: u64 = 8;
        pub const RESUMED: u64 = 9;
        pub const CLIENT_DISCONNECT: u64 = 13;
    }

    /// Track voice state for users in channels.
    #[derive(Debug, Clone)]
    pub struct VoiceState {
        pub user_id: String,
        pub channel_id: Option<String>,
        pub guild_id: String,
        pub self_mute: bool,
        pub self_deaf: bool,
    }

    /// Represents a pending or active voice connection.
    #[derive(Debug, Clone)]
    pub struct VoiceConnection {
        pub guild_id: String,
        pub channel_id: String,
        pub session_id: Option<String>,
        pub endpoint: Option<String>,
        pub token: Option<String>,
        pub ssrc: Option<u32>,
    }

    impl VoiceConnection {
        pub fn new(guild_id: String, channel_id: String) -> Self {
            Self {
                guild_id,
                channel_id,
                session_id: None,
                endpoint: None,
                token: None,
                ssrc: None,
            }
        }

        pub fn is_ready(&self) -> bool {
            self.session_id.is_some() && self.endpoint.is_some() && self.token.is_some()
        }
    }

    /// Event sent when a user finishes speaking and audio is ready for processing.
    #[derive(Debug)]
    pub struct AudioCompleteEvent {
        pub ssrc: u32,
        pub user_id: Option<u64>,
        pub samples: Vec<i16>,
    }

    /// Manages audio reception from voice channels.
    /// Buffers decoded PCM per SSRC, flushes when a user stops speaking.
    pub struct AudioReceiver {
        /// PCM buffers keyed by SSRC
        buffers: Arc<parking_lot::Mutex<HashMap<u32, Vec<i16>>>>,
        /// SSRC → UserId mapping (learned from SpeakingStateUpdate)
        ssrc_map: Arc<parking_lot::Mutex<HashMap<u32, u64>>>,
        /// Channel to send completed audio buffers for processing
        tx: mpsc::UnboundedSender<AudioCompleteEvent>,
    }

    impl AudioReceiver {
        pub fn new(tx: mpsc::UnboundedSender<AudioCompleteEvent>) -> Self {
            Self {
                buffers: Arc::new(parking_lot::Mutex::new(HashMap::new())),
                ssrc_map: Arc::new(parking_lot::Mutex::new(HashMap::new())),
                tx,
            }
        }
    }

    impl Clone for AudioReceiver {
        fn clone(&self) -> Self {
            Self {
                buffers: Arc::clone(&self.buffers),
                ssrc_map: Arc::clone(&self.ssrc_map),
                tx: self.tx.clone(),
            }
        }
    }

    #[async_trait::async_trait]
    impl SongbirdEventHandler for AudioReceiver {
        async fn act(&self, ctx: &EventContext<'_>) -> Option<Event> {
            match ctx {
                EventContext::VoiceTick(tick) => {
                    // Accumulate decoded PCM for each speaking user
                    for (&ssrc, voice_data) in tick.speaking.iter() {
                        if let Some(ref decoded) = voice_data.decoded_voice {
                            if !decoded.is_empty() {
                                let mut bufs = self.buffers.lock();
                                bufs.entry(ssrc)
                                    .or_insert_with(Vec::new)
                                    .extend_from_slice(decoded);
                            }
                        }
                    }
                    None
                }
                EventContext::SpeakingStateUpdate(speaking) => {
                    // Map SSRC to user ID
                    if let Some(uid) = speaking.user_id {
                        let mut map = self.ssrc_map.lock();
                        map.insert(speaking.ssrc, uid.0);
                    }

                    // When user stops speaking (microphone flag cleared), flush their buffer
                    if !speaking.speaking.microphone() {
                        let ssrc = speaking.ssrc;
                        let mut bufs = self.buffers.lock();
                        if let Some(samples) = bufs.remove(&ssrc) {
                            // Only process if we have meaningful audio (>200ms at 48kHz stereo)
                            if samples.len() > 48000 * 2 {
                                let user_id = {
                                    let map = self.ssrc_map.lock();
                                    map.get(&ssrc).copied()
                                };
                                let _ = self.tx.send(AudioCompleteEvent {
                                    ssrc,
                                    user_id,
                                    samples,
                                });
                            }
                        }
                    }
                    None
                }
                EventContext::ClientDisconnect(dc) => {
                    // Clean up buffers for disconnected client
                    debug!("Voice: client disconnected, user_id={:?}", dc.user_id);
                    None
                }
                _ => None,
            }
        }
    }

    /// Wraps a Songbird Driver for a single voice connection.
    pub struct SongbirdVoiceManager {
        pub driver: Driver,
    }

    impl SongbirdVoiceManager {
        pub fn new() -> Self {
            let config = SongbirdConfig::default()
                .decode_mode(DecodeMode::Decode(songbird::driver::DecodeConfig::default()));

            Self {
                driver: Driver::new(config),
            }
        }

        /// Connect to a Discord voice channel using the gateway-provided info.
        pub async fn connect(
            &mut self,
            conn: &VoiceConnection,
            bot_user_id: u64,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let guild_nz = NonZeroU64::new(conn.guild_id.parse::<u64>()?)
                .ok_or("guild_id is zero")?;
            let channel_nz = NonZeroU64::new(conn.channel_id.parse::<u64>()?)
                .ok_or("channel_id is zero")?;
            let user_nz = NonZeroU64::new(bot_user_id)
                .ok_or("bot_user_id is zero")?;

            let endpoint = conn.endpoint.as_deref()
                .ok_or("missing voice endpoint")?
                .to_string();
            let session_id = conn.session_id.as_deref()
                .ok_or("missing voice session_id")?
                .to_string();
            let token = conn.token.as_deref()
                .ok_or("missing voice token")?
                .to_string();

            let info = ConnectionInfo {
                channel_id: Some(ChannelId(channel_nz)),
                endpoint,
                guild_id: GuildId(guild_nz),
                session_id,
                token,
                user_id: UserId(user_nz),
            };

            info!("Voice: connecting to guild={}, channel={}", conn.guild_id, conn.channel_id);
            self.driver.connect(info).await?;
            info!("Voice: connected successfully");

            Ok(())
        }

        /// Register an event handler on the driver.
        pub fn register_event<F: SongbirdEventHandler + 'static>(
            &mut self,
            event: Event,
            handler: F,
        ) {
            self.driver.add_global_event(event, handler);
        }

        /// Play an audio file through the voice connection.
        pub fn play_file(&mut self, path: &str) {
            let file = AudioFile::new(path.to_string());
            let _handle = self.driver.play_input(file.into());
            info!("Voice: playing audio from {path}");
        }

        /// Disconnect from voice.
        pub fn disconnect(&mut self) {
            self.driver.leave();
            info!("Voice: disconnected");
        }
    }

    /// Save raw PCM samples (48kHz stereo i16) to a WAV file.
    pub fn save_pcm_to_wav(
        path: &str,
        samples: &[i16],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use std::io::Write;

        let sample_rate: u32 = 48000;
        let channels: u16 = 2;
        let bits_per_sample: u16 = 16;
        let byte_rate = sample_rate * channels as u32 * bits_per_sample as u32 / 8;
        let block_align = channels * bits_per_sample / 8;
        let data_size = (samples.len() as u32) * 2;
        let file_size = 36 + data_size;

        let mut f = std::fs::File::create(path)?;
        f.write_all(b"RIFF")?;
        f.write_all(&file_size.to_le_bytes())?;
        f.write_all(b"WAVE")?;
        f.write_all(b"fmt ")?;
        f.write_all(&16u32.to_le_bytes())?;
        f.write_all(&1u16.to_le_bytes())?;
        f.write_all(&channels.to_le_bytes())?;
        f.write_all(&sample_rate.to_le_bytes())?;
        f.write_all(&byte_rate.to_le_bytes())?;
        f.write_all(&block_align.to_le_bytes())?;
        f.write_all(&bits_per_sample.to_le_bytes())?;
        f.write_all(b"data")?;
        f.write_all(&data_size.to_le_bytes())?;
        for s in samples {
            f.write_all(&s.to_le_bytes())?;
        }
        Ok(())
    }
}

/// Streaming message state for live updates (Phase 4).
struct StreamingState {
    message_id: String,
    channel_id: String,
    accumulated: String,
    last_edit: Instant,
    phase: String,
}

/// Discord Gateway adapter using WebSocket.
pub struct DiscordAdapter {
    /// SECURITY: Bot token is zeroized on drop to prevent memory disclosure.
    token: Zeroizing<String>,
    client: reqwest::Client,
    allowed_guilds: Vec<String>,
    intents: u64,
    shutdown_tx: Arc<watch::Sender<bool>>,
    shutdown_rx: watch::Receiver<bool>,
    /// Bot's own user ID (populated after READY event).
    bot_user_id: Arc<RwLock<Option<String>>>,
    /// Session ID for resume (populated after READY event).
    session_id: Arc<RwLock<Option<String>>>,
    /// Resume gateway URL.
    resume_gateway_url: Arc<RwLock<Option<String>>>,
    /// Application ID (Phase 3 - slash commands).
    application_id: Arc<RwLock<Option<String>>>,
    /// Voice connections keyed by guild_id (Phase 5).
    voice_connections: Arc<DashMap<String, voice::VoiceConnection>>,
    /// Voice states keyed by user_id (Phase 5).
    voice_states: Arc<DashMap<String, voice::VoiceState>>,
    voice_managers: Arc<DashMap<String, voice::SongbirdVoiceManager>>,
    /// Channel for sending gateway commands (opcode 4 for voice join/leave)
    gateway_cmd_tx: Arc<tokio::sync::RwLock<Option<tokio::sync::mpsc::UnboundedSender<String>>>>,
    /// Streaming messages state (Phase 4).
    streaming_messages: Arc<DashMap<String, StreamingState>>,
}

impl DiscordAdapter {
    pub fn new(token: String, allowed_guilds: Vec<String>, intents: u64) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            token: Zeroizing::new(token),
            client: reqwest::Client::new(),
            allowed_guilds,
            intents,
            shutdown_tx: Arc::new(shutdown_tx),
            shutdown_rx,
            bot_user_id: Arc::new(RwLock::new(None)),
            session_id: Arc::new(RwLock::new(None)),
            resume_gateway_url: Arc::new(RwLock::new(None)),
            application_id: Arc::new(RwLock::new(None)),
            voice_connections: Arc::new(DashMap::new()),
            voice_states: Arc::new(DashMap::new()),
            voice_managers: Arc::new(DashMap::new()),
            gateway_cmd_tx: Arc::new(tokio::sync::RwLock::new(None)),
            streaming_messages: Arc::new(DashMap::new()),
        }
    }

    /// Get the WebSocket gateway URL from the Discord API.
    async fn get_gateway_url(&self) -> Result<String, Box<dyn std::error::Error>> {
        let url = format!("{DISCORD_API_BASE}/gateway/bot");
        let resp: serde_json::Value = self
            .client
            .get(&url)
            .header("Authorization", format!("Bot {}", self.token.as_str()))
            .send()
            .await?
            .json()
            .await?;

        let ws_url = resp["url"]
            .as_str()
            .ok_or("Missing 'url' in gateway response")?;

        Ok(format!("{ws_url}/?v=10&encoding=json"))
    }

    /// Send a message to a Discord channel via REST API.
    async fn api_send_message(
        &self,
        channel_id: &str,
        text: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!("{DISCORD_API_BASE}/channels/{channel_id}/messages");
        let chunks = split_message(text, DISCORD_MSG_LIMIT);

        for chunk in chunks {
            let body = serde_json::json!({ "content": chunk });
            let resp = self
                .client
                .post(&url)
                .header("Authorization", format!("Bot {}", self.token.as_str()))
                .json(&body)
                .send()
                .await?;

            if !resp.status().is_success() {
                let body_text = resp.text().await.unwrap_or_default();
                warn!("Discord sendMessage failed: {body_text}");
            }
        }
        Ok(())
    }

    /// Send typing indicator to a Discord channel.
    async fn api_send_typing(&self, channel_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!("{DISCORD_API_BASE}/channels/{channel_id}/typing");
        let _ = self
            .client
            .post(&url)
            .header("Authorization", format!("Bot {}", self.token.as_str()))
            .send()
            .await?;
        Ok(())
    }

    // ============= PHASE 1: Rich Output (Embeds, Files, Images) =============

    /// Send a message with an embed.
    async fn api_send_embed(
        &self,
        channel_id: &str,
        text: Option<&str>,
        embed: &discord_types::DiscordEmbed,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let url = format!("{DISCORD_API_BASE}/channels/{channel_id}/messages");
        let mut body = serde_json::json!({ "embeds": [embed] });
        if let Some(t) = text {
            body["content"] = serde_json::Value::String(t.to_string());
        }
        let resp = self
            .client
            .post(&url)
            .header("Authorization", format!("Bot {}", self.token.as_str()))
            .json(&body)
            .send()
            .await?;
        let resp_json: serde_json::Value = resp.json().await.unwrap_or_default();
        Ok(resp_json["id"]
            .as_str()
            .unwrap_or("0")
            .to_string())
    }

    /// Upload a file as an attachment via multipart form.
    async fn api_send_file(
        &self,
        channel_id: &str,
        file_bytes: Vec<u8>,
        filename: &str,
        caption: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!("{DISCORD_API_BASE}/channels/{channel_id}/messages");
        let part = reqwest::multipart::Part::bytes(file_bytes)
            .file_name(filename.to_string())
            .mime_str("application/octet-stream")?;
        let mut form = reqwest::multipart::Form::new().part("files[0]", part);
        if let Some(cap) = caption {
            let payload = serde_json::json!({ "content": cap });
            form = form.text("payload_json", serde_json::to_string(&payload)?);
        }
        let resp = self
            .client
            .post(&url)
            .header("Authorization", format!("Bot {}", self.token.as_str()))
            .multipart(form)
            .send()
            .await?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            warn!("Discord sendFile failed: {body}");
        }
        Ok(())
    }

    /// Send an image as an embed with the image URL.
    async fn api_send_image_embed(
        &self,
        channel_id: &str,
        image_url: &str,
        caption: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let embed = discord_types::DiscordEmbed {
            image: Some(discord_types::EmbedMedia {
                url: image_url.to_string(),
            }),
            description: caption.map(|c| c.to_string()),
            color: Some(0x5865F2), // Discord blurple
            ..Default::default()
        };
        self.api_send_embed(channel_id, None, &embed).await?;
        Ok(())
    }

    /// Create a message and return its ID (for later editing).
    async fn api_create_message(
        &self,
        channel_id: &str,
        text: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let url = format!("{DISCORD_API_BASE}/channels/{channel_id}/messages");
        let body = serde_json::json!({ "content": text });
        let resp = self
            .client
            .post(&url)
            .header("Authorization", format!("Bot {}", self.token.as_str()))
            .json(&body)
            .send()
            .await?;
        let resp_json: serde_json::Value = resp.json().await.unwrap_or_default();
        Ok(resp_json["id"]
            .as_str()
            .unwrap_or("0")
            .to_string())
    }

    /// Edit an existing message.
    async fn api_edit_message(
        &self,
        channel_id: &str,
        message_id: &str,
        text: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!("{DISCORD_API_BASE}/channels/{channel_id}/messages/{message_id}");
        let chunks = split_message(text, DISCORD_MSG_LIMIT);
        // Edit the original with the first chunk
        let body = serde_json::json!({ "content": chunks[0] });
        let resp = self
            .client
            .patch(&url)
            .header("Authorization", format!("Bot {}", self.token.as_str()))
            .json(&body)
            .send()
            .await?;
        if !resp.status().is_success() {
            let body_text = resp.text().await.unwrap_or_default();
            warn!("Discord editMessage failed: {body_text}");
        }
        // Send remaining chunks as new messages
        for chunk in &chunks[1..] {
            self.api_send_message(channel_id, chunk).await?;
        }
        Ok(())
    }

    /// Add a reaction emoji to a message.
    async fn api_add_reaction(
        &self,
        channel_id: &str,
        message_id: &str,
        emoji: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let encoded = url_encode_emoji(emoji);
        let url = format!(
            "{DISCORD_API_BASE}/channels/{channel_id}/messages/{message_id}/reactions/{encoded}/@me"
        );
        let _ = self
            .client
            .put(&url)
            .header("Authorization", format!("Bot {}", self.token.as_str()))
            .header("Content-Length", "0")
            .send()
            .await?;
        Ok(())
    }

    /// Remove a reaction emoji from a message.
    async fn api_remove_reaction(
        &self,
        channel_id: &str,
        message_id: &str,
        emoji: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let encoded = url_encode_emoji(emoji);
        let url = format!(
            "{DISCORD_API_BASE}/channels/{channel_id}/messages/{message_id}/reactions/{encoded}/@me"
        );
        let _ = self
            .client
            .delete(&url)
            .header("Authorization", format!("Bot {}", self.token.as_str()))
            .send()
            .await?;
        Ok(())
    }

    /// Remove all our reactions from a message.
    async fn api_remove_all_own_reactions(
        &self,
        channel_id: &str,
        message_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!(
            "{DISCORD_API_BASE}/channels/{channel_id}/messages/{message_id}/reactions/@me"
        );
        let _ = self
            .client
            .delete(&url)
            .header("Authorization", format!("Bot {}", self.token.as_str()))
            .send()
            .await?;
        Ok(())
    }


    // ============= PHASE 6: Components V2 + Rich Blocks =============

    /// Send a Components V2 message. Uses the IS_COMPONENTS_V2 flag (1 << 15 = 32768).
    /// This disables content/embeds/stickers fields — all content must be in components.
    async fn api_send_v2(
        &self,
        channel_id: &str,
        components: Vec<serde_json::Value>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!("{DISCORD_API_BASE}/channels/{channel_id}/messages");
        let body = serde_json::json!({
            "flags": 32768,
            "components": components
        });
        let resp = self
            .client
            .post(&url)
            .header("Authorization", format!("Bot {}", self.token.as_str()))
            .json(&body)
            .send()
            .await?;
        if !resp.status().is_success() {
            let body_text = resp.text().await.unwrap_or_default();
            warn!("Discord V2 send failed: {body_text}");
            return Err(format!("V2 send failed: {body_text}").into());
        }
        Ok(())
    }

    /// Route rich blocks to the best Discord API method.
    /// Strategy: single embed → legacy embed, buttons → legacy action rows,
    /// mixed/complex → Components V2, with fallback to plain text.
    async fn send_rich_blocks(
        &self,
        channel_id: &str,
        blocks: &[RichBlock],
        fallback_text: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Classify what we have
        let has_embed = blocks.iter().any(|b| matches!(b, RichBlock::Embed { .. }));
        let has_buttons = blocks.iter().any(|b| matches!(b, RichBlock::Buttons { .. }));
        let has_gallery = blocks.iter().any(|b| matches!(b, RichBlock::ImageGallery { .. }));
        let has_section = blocks.iter().any(|b| matches!(b, RichBlock::Section { .. }));
        let plain_blocks: Vec<&str> = blocks.iter().filter_map(|b| {
            if let RichBlock::PlainText(t) = b { Some(t.as_str()) } else { None }
        }).collect();

        // Strategy 1: Embeds (most compatible — works on all platforms including iOS)
        // Send each embed as a separate message for best compatibility
        if has_embed && !has_gallery && !has_section {
            for block in blocks {
                match block {
                    RichBlock::Embed { title, description, color, fields, image_url, footer } => {
                        let mut embed = discord_types::DiscordEmbed {
                            title: title.clone(),
                            description: Some(description.clone()),
                            color: *color,
                            ..Default::default()
                        };
                        if !fields.is_empty() {
                            embed.fields = fields.iter().map(|(n, v, inline)| {
                                discord_types::EmbedField {
                                    name: n.clone(),
                                    value: v.clone(),
                                    inline: Some(*inline),
                                }
                            }).collect();
                        }
                        if let Some(img) = image_url {
                            embed.image = Some(discord_types::EmbedMedia { url: img.clone() });
                        }
                        if let Some(ft) = footer {
                            embed.footer = Some(discord_types::EmbedFooter {
                                text: ft.clone(),
                                icon_url: None,
                            });
                        }
                        // If there are buttons, send embed first, then buttons
                        let companion_text = if !plain_blocks.is_empty() {
                            Some(plain_blocks.join("
"))
                        } else {
                            None
                        };
                        self.api_send_embed(channel_id, companion_text.as_deref(), &embed).await?;
                    }
                    RichBlock::Buttons { text, buttons } => {
                        let components: Vec<crate::types::MessageComponent> = buttons.iter().enumerate().map(|(i, (label, id))| {
                            crate::types::MessageComponent::Button {
                                label: label.clone(),
                                custom_id: id.clone(),
                                style: if i == 0 { 1 } else { 2 }, // first button primary, rest secondary
                            }
                        }).collect();
                        self.api_send_with_components(channel_id, text, &components).await?;
                    }
                    RichBlock::PlainText(text) => {
                        // Already handled as companion text
                    }
                    _ => {}
                }
            }
            return Ok(());
        }

        // Strategy 2: Buttons only — legacy action rows
        if has_buttons && !has_embed && !has_gallery && !has_section {
            for block in blocks {
                match block {
                    RichBlock::Buttons { text, buttons } => {
                        let components: Vec<crate::types::MessageComponent> = buttons.iter().enumerate().map(|(i, (label, id))| {
                            crate::types::MessageComponent::Button {
                                label: label.clone(),
                                custom_id: id.clone(),
                                style: if i == 0 { 1 } else { 2 },
                            }
                        }).collect();
                        self.api_send_with_components(channel_id, text, &components).await?;
                    }
                    RichBlock::PlainText(text) => {
                        self.api_send_message(channel_id, text).await?;
                    }
                    _ => {}
                }
            }
            return Ok(());
        }




        // Strategy 3: Components V2 for complex layouts
        let mut v2_components: Vec<serde_json::Value> = Vec::new();

        for block in blocks {
            match block {
                RichBlock::PlainText(text) => {
                    if !text.trim().is_empty() {
                        v2_components.push(serde_json::json!({
                            "type": 10,
                            "content": text
                        }));
                    }
                }
                RichBlock::Embed { title, description, color, fields, image_url, footer } => {
                    // Wrap embed content in a Container with accent color
                    let accent = color.unwrap_or(0x5865F2);
                    let mut inner: Vec<serde_json::Value> = Vec::new();

                    // Title + description as TextDisplay
                    let mut md = String::new();
                    if let Some(t) = title {
                        md.push_str(&format!("### {}
", t));
                    }
                    md.push_str(description);

                    if !fields.is_empty() {
                        md.push_str("

");
                        for (name, value, _inline) in fields {
                            md.push_str(&format!("**{}:** {}
", name, value));
                        }
                    }

                    if let Some(ft) = footer {
                        md.push_str(&format!("
-# {}", ft));
                    }

                    inner.push(serde_json::json!({
                        "type": 10,
                        "content": md
                    }));

                    if let Some(img) = image_url {
                        inner.push(serde_json::json!({
                            "type": 12,
                            "items": [{
                                "media": { "url": img }
                            }]
                        }));
                    }

                    v2_components.push(serde_json::json!({
                        "type": 17,
                        "accent_color": accent,
                        "components": inner
                    }));
                }
                RichBlock::Buttons { text, buttons } => {
                    if !text.trim().is_empty() {
                        v2_components.push(serde_json::json!({
                            "type": 10,
                            "content": text
                        }));
                    }
                    let btns: Vec<serde_json::Value> = buttons.iter().enumerate().map(|(i, (label, id))| {
                        serde_json::json!({
                            "type": 2,
                            "style": if i == 0 { 1 } else { 2 },
                            "label": label,
                            "custom_id": id
                        })
                    }).collect();
                    v2_components.push(serde_json::json!({
                        "type": 1,
                        "components": btns
                    }));
                }
                RichBlock::ImageGallery { images } => {
                    let items: Vec<serde_json::Value> = images.iter().map(|(url, caption)| {
                        let mut item = serde_json::json!({
                            "media": { "url": url }
                        });
                        if let Some(cap) = caption {
                            item["description"] = serde_json::Value::String(cap.clone());
                        }
                        item
                    }).collect();
                    v2_components.push(serde_json::json!({
                        "type": 12,
                        "items": items
                    }));
                }
                RichBlock::Section { text, thumbnail_url } => {
                    let mut section = serde_json::json!({
                        "type": 9,
                        "components": [{
                            "type": 10,
                            "content": text
                        }]
                    });
                    if let Some(thumb) = thumbnail_url {
                        section["accessory"] = serde_json::json!({
                            "type": 11,
                            "media": { "url": thumb }
                        });
                    }
                    v2_components.push(section);
                }
            }
        }

        // Try V2 first, fallback to plain text on failure
        let v2_ok = {
            let result = self.api_send_v2(channel_id, v2_components).await;
            if let Err(e) = &result {
                warn!("Discord V2 failed ({e}), falling back to plain text");
            }
            result.is_ok()
        }; // v2_result dropped here, before next await
        if v2_ok {
            return Ok(());
        }

        // V2 failed — fallback: send each block as individual legacy embed
        let mut any_sent = false;
        for block in blocks {
            match block {
                RichBlock::Embed { title, description, color, fields, image_url, footer } => {
                    let mut embed = discord_types::DiscordEmbed {
                        title: title.clone(),
                        description: Some(if description.len() > 4096 {
                            format!("{}...", &description[..4090])
                        } else {
                            description.clone()
                        }),
                        color: *color,
                        ..Default::default()
                    };
                    if !fields.is_empty() {
                        embed.fields = fields.iter().take(25).map(|(n, v, inline)| {
                            discord_types::EmbedField {
                                name: if n.len() > 256 { format!("{}...", &n[..253]) } else { n.clone() },
                                value: if v.len() > 1024 { format!("{}...", &v[..1021]) } else { v.clone() },
                                inline: Some(*inline),
                            }
                        }).collect();
                    }
                    if let Some(img) = image_url {
                        embed.image = Some(discord_types::EmbedMedia { url: img.clone() });
                    }
                    if let Some(ft) = footer {
                        embed.footer = Some(discord_types::EmbedFooter {
                            text: ft.clone(),
                            icon_url: None,
                        });
                    }
                    if let Err(e) = self.api_send_embed(channel_id, None, &embed).await {
                        warn!("Failed to send embed: {e}");
                    } else {
                        any_sent = true;
                    }
                }
                RichBlock::PlainText(t) => {
                    if !t.trim().is_empty() {
                        // Chunk plain text to 2000 chars
                        let mut rem = t.as_str();
                        while !rem.is_empty() {
                            let end = if rem.len() <= 2000 {
                                rem.len()
                            } else {
                                let mut e = 2000;
                                while e > 0 && !rem.is_char_boundary(e) { e -= 1; }
                                rem[..e].rfind('\n').map(|p| p + 1).unwrap_or(e)
                            };
                            let _ = self.api_send_message(channel_id, &rem[..end]).await;
                            any_sent = true;
                            rem = &rem[end..];
                        }
                    }
                }
                RichBlock::Buttons { text, buttons } => {
                    let components: Vec<crate::types::MessageComponent> = buttons.iter().enumerate().map(|(i, (label, id))| {
                        crate::types::MessageComponent::Button {
                            label: label.clone(),
                            custom_id: id.clone(),
                            style: if i == 0 { 1 } else { 2 },
                        }
                    }).collect();
                    let _ = self.api_send_with_components(channel_id, text, &components).await;
                    any_sent = true;
                }
                _ => {}
            }
        }

        if any_sent {
            Ok(())
        } else {
            // Absolute last resort: chunked plain fallback text
            let mut rem = fallback_text;
            while !rem.is_empty() {
                let end = if rem.len() <= 2000 {
                    rem.len()
                } else {
                    let mut e = 2000;
                    while e > 0 && !rem.is_char_boundary(e) { e -= 1; }
                    rem[..e].rfind('\n').map(|p| p + 1).unwrap_or(e)
                };
                let _ = self.api_send_message(channel_id, &rem[..end]).await;
                rem = &rem[end..];
            }
            Ok(())
        }
    }

    // ============= PHASE 2: Interactive Components =============

    /// Send a message with interactive components (buttons, select menus).
    async fn api_send_with_components(
        &self,
        channel_id: &str,
        text: &str,
        components: &[crate::types::MessageComponent],
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::types::MessageComponent;
        let url = format!("{DISCORD_API_BASE}/channels/{channel_id}/messages");

        let discord_components: Vec<discord_types::Component> = components
            .iter()
            .map(|c| match c {
                MessageComponent::Button {
                    label,
                    custom_id,
                    style,
                } => discord_types::Component::Button {
                    component_type: 2,
                    style: *style,
                    label: label.clone(),
                    custom_id: custom_id.clone(),
                },
                MessageComponent::SelectMenu {
                    custom_id,
                    placeholder,
                    options,
                    min_values,
                    max_values,
                } => discord_types::Component::SelectMenu {
                    component_type: 3,
                    custom_id: custom_id.clone(),
                    placeholder: placeholder.clone(),
                    options: options
                        .iter()
                        .map(|o| discord_types::SelectOption {
                            label: o.label.clone(),
                            value: o.value.clone(),
                            description: o.description.clone(),
                            default: None,
                        })
                        .collect(),
                    min_values: Some(*min_values),
                    max_values: Some(*max_values),
                },
            })
            .collect();

        // Group into action rows (max 5 buttons per row, 1 select per row)
        let mut action_rows = Vec::new();
        let mut current_button_row = Vec::new();
        for comp in discord_components {
            match &comp {
                discord_types::Component::Button { .. } => {
                    current_button_row.push(comp);
                    if current_button_row.len() >= 5 {
                        action_rows.push(discord_types::ActionRow {
                            component_type: 1,
                            components: std::mem::take(&mut current_button_row),
                        });
                    }
                }
                discord_types::Component::SelectMenu { .. } => {
                    if !current_button_row.is_empty() {
                        action_rows.push(discord_types::ActionRow {
                            component_type: 1,
                            components: std::mem::take(&mut current_button_row),
                        });
                    }
                    action_rows.push(discord_types::ActionRow {
                        component_type: 1,
                        components: vec![comp],
                    });
                }
            }
        }
        if !current_button_row.is_empty() {
            action_rows.push(discord_types::ActionRow {
                component_type: 1,
                components: current_button_row,
            });
        }

        let body = serde_json::json!({
            "content": text,
            "components": action_rows,
        });

        let resp = self
            .client
            .post(&url)
            .header("Authorization", format!("Bot {}", self.token.as_str()))
            .json(&body)
            .send()
            .await?;
        if !resp.status().is_success() {
            let body_text = resp.text().await.unwrap_or_default();
            warn!("Discord sendWithComponents failed: {body_text}");
        }
        Ok(())
    }

    /// ACK an interaction (deferred update, no visible response).
    async fn api_ack_interaction(
        &self,
        interaction_id: &str,
        interaction_token: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!(
            "{DISCORD_API_BASE}/interactions/{interaction_id}/{interaction_token}/callback"
        );
        let body = serde_json::json!({ "type": 6 }); // DEFERRED_UPDATE_MESSAGE
        self.client
            .post(&url)
            .json(&body)
            .send()
            .await?;
        Ok(())
    }

    /// Respond to an interaction with a message.
    async fn api_interaction_respond(
        &self,
        interaction_id: &str,
        interaction_token: &str,
        content: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!(
            "{DISCORD_API_BASE}/interactions/{interaction_id}/{interaction_token}/callback"
        );
        let body = serde_json::json!({
            "type": 4, // CHANNEL_MESSAGE_WITH_SOURCE
            "data": { "content": content }
        });
        self.client
            .post(&url)
            .json(&body)
            .send()
            .await?;
        Ok(())
    }

    /// Respond to an interaction with a modal dialog.
    async fn api_send_modal(
        &self,
        interaction_id: &str,
        interaction_token: &str,
        custom_id: &str,
        title: &str,
        fields: &[(&str, &str, bool)],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!(
            "{DISCORD_API_BASE}/interactions/{interaction_id}/{interaction_token}/callback"
        );
        let components: Vec<serde_json::Value> = fields
            .iter()
            .map(|(id, label, required)| {
                serde_json::json!({
                    "type": 1,
                    "components": [{
                        "type": 4, // TEXT_INPUT
                        "custom_id": id,
                        "label": label,
                        "style": 2, // PARAGRAPH
                        "required": required,
                    }]
                })
            })
            .collect();
        let body = serde_json::json!({
            "type": 9, // MODAL
            "data": {
                "custom_id": custom_id,
                "title": title,
                "components": components,
            }
        });
        self.client
            .post(&url)
            .json(&body)
            .send()
            .await?;
        Ok(())
    }

    // ============= PHASE 4: Streaming (Live Message Editing) =============

    /// Stream an agent response with live message editing via SSE.
    async fn stream_agent_response(
        &self,
        channel_id: &str,
        agent_id: &str,
        user_message: &str,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let api_url = format!("http://127.0.0.1:4200/api/agents/{agent_id}/message/stream");
        let api_key = std::env::var("OPENFANG_API_KEY").unwrap_or_default();

        let resp = self
            .client
            .post(&api_url)
            .header("Authorization", format!("Bearer {api_key}"))
            .header("Accept", "text/event-stream")
            .json(&serde_json::json!({ "message": user_message }))
            .send()
            .await?;

        if !resp.status().is_success() {
            return Ok(false); // Fall back to non-streaming
        }

        // Create initial "thinking" message
        let msg_id = self.api_create_message(channel_id, "🤔 *Thinking...*").await?;
        let _ = self.api_add_reaction(channel_id, &msg_id, "🤔").await;

        let mut accumulated = String::new();
        let mut last_edit = Instant::now();
        let mut current_phase = "thinking";
        let edit_interval = Duration::from_millis(800); // Rate limit edits

        let mut stream = resp.bytes_stream();
        let mut buffer = String::new();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            buffer.push_str(&String::from_utf8_lossy(&chunk));

            // Parse SSE events from buffer
            while let Some(event_end) = buffer.find("\n\n") {
                let event_block = buffer[..event_end].to_string();
                buffer = buffer[event_end + 2..].to_string();

                let mut event_type = String::new();
                let mut event_data = String::new();
                for line in event_block.lines() {
                    if let Some(rest) = line.strip_prefix("event: ") {
                        event_type = rest.to_string();
                    } else if let Some(rest) = line.strip_prefix("data: ") {
                        event_data = rest.to_string();
                    }
                }

                match event_type.as_str() {
                    "chunk" => {
                        if let Ok(data) = serde_json::from_str::<serde_json::Value>(&event_data)
                        {
                            if let Some(text) = data["content"].as_str() {
                                accumulated.push_str(text);
                                // Edit message at intervals to avoid rate limiting
                                if last_edit.elapsed() >= edit_interval {
                                    let display = if accumulated.len() > DISCORD_MSG_LIMIT - 20
                                    {
                                        format!("{}...", &accumulated[..DISCORD_MSG_LIMIT - 20])
                                    } else {
                                        format!("{accumulated}▌") // cursor
                                    };
                                    let _ = self.api_edit_message(channel_id, &msg_id, &display)
                                        .await;
                                    last_edit = Instant::now();
                                }
                            }
                        }
                    }
                    "tool_use" => {
                        if let Ok(data) = serde_json::from_str::<serde_json::Value>(&event_data)
                        {
                            if let Some(tool) = data["tool"].as_str() {
                                if current_phase != "tool_use" {
                                    let _ = self
                                        .api_remove_reaction(channel_id, &msg_id, "🤔")
                                        .await;
                                    let _ = self
                                        .api_add_reaction(channel_id, &msg_id, "⚙️")
                                        .await;
                                    current_phase = "tool_use";
                                }
                                let status = format!("{accumulated}\n\n⚙️ *Using {tool}...*");
                                let _ = self.api_edit_message(channel_id, &msg_id, &status).await;
                                last_edit = Instant::now();
                            }
                        }
                    }
                    "phase" => {
                        if let Ok(data) = serde_json::from_str::<serde_json::Value>(&event_data)
                        {
                            let phase = data["phase"].as_str().unwrap_or("");
                            match phase {
                                "streaming" if current_phase != "streaming" => {
                                    let _ = self
                                        .api_remove_reaction(channel_id, &msg_id, "⚙️")
                                        .await;
                                    let _ = self
                                        .api_add_reaction(channel_id, &msg_id, "✍️")
                                        .await;
                                    current_phase = "streaming";
                                }
                                _ => {}
                            }
                        }
                    }
                    "done" => {
                        // Final edit with complete text
                        let _ = self
                            .api_remove_reaction(channel_id, &msg_id, "🤔")
                            .await;
                        let _ = self
                            .api_remove_reaction(channel_id, &msg_id, "⚙️")
                            .await;
                        let _ = self
                            .api_remove_reaction(channel_id, &msg_id, "✍️")
                            .await;

                        if accumulated.is_empty() {
                            accumulated = "*(No response)*".to_string();
                        }

                        // Handle messages > 2000 chars
                        let chunks = split_message(&accumulated, DISCORD_MSG_LIMIT);
                        let _ = self.api_edit_message(channel_id, &msg_id, chunks[0]).await;
                        for chunk in &chunks[1..] {
                            self.api_send_message(channel_id, chunk).await?;
                        }
                        let _ = self
                            .api_add_reaction(channel_id, &msg_id, "✅")
                            .await;
                        return Ok(true);
                    }
                    _ => {}
                }
            }
        }

        // Stream ended without "done" — finalize what we have
        if !accumulated.is_empty() {
            let chunks = split_message(&accumulated, DISCORD_MSG_LIMIT);
            let _ = self.api_edit_message(channel_id, &msg_id, chunks[0]).await;
            for chunk in &chunks[1..] {
                self.api_send_message(channel_id, chunk).await?;
            }
        }
        let _ = self
            .api_add_reaction(channel_id, &msg_id, "✅")
            .await;
        Ok(true)
    }
}

#[async_trait]
impl ChannelAdapter for DiscordAdapter {
    fn name(&self) -> &str {
        "discord"
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::Discord
    }

    async fn start(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = ChannelMessage> + Send>>, Box<dyn std::error::Error>>
    {
        let gateway_url = self.get_gateway_url().await?;
        info!("Discord gateway URL obtained");

        let (tx, rx) = mpsc::channel::<ChannelMessage>(256);

        let token = self.token.clone();
        let intents = self.intents;
        let allowed_guilds = self.allowed_guilds.clone();
        let bot_user_id = self.bot_user_id.clone();
        let session_id_store = self.session_id.clone();
        let resume_url_store = self.resume_gateway_url.clone();
        let application_id = self.application_id.clone();
        let voice_connections = self.voice_connections.clone();
        let voice_states = self.voice_states.clone();
        let voice_managers = self.voice_managers.clone();
        let gateway_cmd_tx = self.gateway_cmd_tx.clone();
        let mut shutdown = self.shutdown_rx.clone();

        tokio::spawn(async move {
            let mut backoff = INITIAL_BACKOFF;
            let mut connect_url = gateway_url;
            // Sequence persists across reconnections for RESUME
            let sequence: Arc<RwLock<Option<u64>>> = Arc::new(RwLock::new(None));

            loop {
                if *shutdown.borrow() {
                    break;
                }

                info!("Connecting to Discord gateway...");

                let ws_result = tokio_tungstenite::connect_async(&connect_url).await;
                let ws_stream = match ws_result {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        warn!(
                            "Discord gateway connection failed: {e}, retrying in {backoff:?}"
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };

                backoff = INITIAL_BACKOFF;
                info!("Discord gateway connected");

                let (mut ws_tx, mut ws_rx) = ws_stream.split();
                // Heartbeat state: timer starts at a large interval, gets reset on HELLO
                let mut heartbeat_timer = tokio::time::interval(Duration::from_secs(86400));
                heartbeat_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                let mut heartbeat_active = false;
                let mut awaiting_heartbeat_ack = false;

                // Inner message loop — returns true if we should reconnect
                let should_reconnect = 'inner: loop {
                    let msg = tokio::select! {
                        msg = ws_rx.next() => msg,
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                info!("Discord shutdown requested");
                                let _ = ws_tx.close().await;
                                return;
                            }
                            continue;
                        }
                        _ = heartbeat_timer.tick(), if heartbeat_active => {
                            // Periodic heartbeat
                            if awaiting_heartbeat_ack {
                                warn!("Discord: missed heartbeat ACK — zombie connection, reconnecting");
                                break 'inner true;
                            }
                            let seq = *sequence.read().await;
                            let hb = serde_json::json!({ "op": opcode::HEARTBEAT, "d": seq });
                            debug!("Discord: sending heartbeat (seq={seq:?})");
                            if let Err(e) = ws_tx
                                .send(tokio_tungstenite::tungstenite::Message::Text(
                                    serde_json::to_string(&hb).unwrap(),
                                ))
                                .await
                            {
                                error!("Discord: failed to send heartbeat: {e}");
                                break 'inner true;
                            }
                            awaiting_heartbeat_ack = true;
                            continue;
                        }
                    };

                    let msg = match msg {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => {
                            warn!("Discord WebSocket error: {e}");
                            break 'inner true;
                        }
                        None => {
                            info!("Discord WebSocket closed");
                            break 'inner true;
                        }
                    };

                    let text = match msg {
                        tokio_tungstenite::tungstenite::Message::Text(t) => t,
                        tokio_tungstenite::tungstenite::Message::Close(_) => {
                            info!("Discord gateway closed by server");
                            break 'inner true;
                        }
                        _ => continue,
                    };

                    let payload: serde_json::Value = match serde_json::from_str(&text) {
                        Ok(v) => v,
                        Err(e) => {
                            warn!("Discord: failed to parse gateway message: {e}");
                            continue;
                        }
                    };

                    let op = payload["op"].as_u64().unwrap_or(999);

                    // Update sequence number
                    if let Some(s) = payload["s"].as_u64() {
                        *sequence.write().await = Some(s);
                    }

                    match op {
                        opcode::HELLO => {
                            let interval =
                                payload["d"]["heartbeat_interval"].as_u64().unwrap_or(45000);
                            // heartbeat interval used below to set timer
                            debug!("Discord HELLO: heartbeat_interval={interval}ms");

                            // Try RESUME if we have a session, otherwise IDENTIFY
                            let has_session = session_id_store.read().await.is_some();
                            let has_seq = sequence.read().await.is_some();

                            let gateway_msg = if has_session && has_seq {
                                let sid = session_id_store.read().await.clone().unwrap();
                                let seq = *sequence.read().await;
                                info!("Discord: sending RESUME (session={sid})");
                                serde_json::json!({
                                    "op": opcode::RESUME,
                                    "d": {
                                        "token": token.as_str(),
                                        "session_id": sid,
                                        "seq": seq
                                    }
                                })
                            } else {
                                info!("Discord: sending IDENTIFY");
                                serde_json::json!({
                                    "op": opcode::IDENTIFY,
                                    "d": {
                                        "token": token.as_str(),
                                        "intents": intents,
                                        "properties": {
                                            "os": "linux",
                                            "browser": "openfang",
                                            "device": "openfang"
                                        }
                                    }
                                })
                            };

                            if let Err(e) = ws_tx
                                .send(tokio_tungstenite::tungstenite::Message::Text(
                                    serde_json::to_string(&gateway_msg).unwrap(),
                                ))
                                .await
                            {
                                error!("Discord: failed to send IDENTIFY/RESUME: {e}");
                                break 'inner true;
                            }

                            // Activate heartbeat timer with jitter per Discord spec
                            let jitter = (interval as f64 * 0.9) as u64; // slight jitter
                            heartbeat_timer =
                                tokio::time::interval(Duration::from_millis(jitter));
                            heartbeat_timer
                                .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                            heartbeat_timer.tick().await; // consume immediate first tick
                            heartbeat_active = true;
                            awaiting_heartbeat_ack = false;
                            info!("Discord: heartbeat timer started (interval={interval}ms, effective={jitter}ms)");
                        }

                        opcode::DISPATCH => {
                            let event_name = payload["t"].as_str().unwrap_or("");
                            let d = &payload["d"];

                            match event_name {
                                "READY" => {
                                    let user_id =
                                        d["user"]["id"].as_str().unwrap_or("").to_string();
                                    let username =
                                        d["user"]["username"].as_str().unwrap_or("unknown");
                                    let sid = d["session_id"].as_str().unwrap_or("").to_string();
                                    let resume_url =
                                        d["resume_gateway_url"].as_str().unwrap_or("").to_string();

                                    *bot_user_id.write().await = Some(user_id.clone());
                                    *session_id_store.write().await = Some(sid);
                                    if !resume_url.is_empty() {
                                        *resume_url_store.write().await = Some(resume_url);
                                    }

                                    // Phase 3: Capture application ID for slash commands
                                    let app_id =
                                        d["application"]["id"].as_str().unwrap_or("").to_string();
                                    if !app_id.is_empty() {
                                        *application_id.write().await = Some(app_id.clone());
                                        // Register slash commands (fire and forget)
                                        let token_clone = token.clone();
                                        let client = reqwest::Client::new();
                                        tokio::spawn(async move {
                                            if let Err(e) =
                                                register_slash_commands(&client, &token_clone, &app_id)
                                                    .await
                                            {
                                                error!("Discord: failed to register slash commands: {e}");
                                            }
                                        });
                                    }

                                    // Parse initial voice states from READY payload
                                    if let Some(guilds) = d["guilds"].as_array() {
                                        let mut vs_count = 0u32;
                                        for guild in guilds {
                                            let gid = guild["id"].as_str().unwrap_or("").to_string();
                                            if let Some(states) = guild["voice_states"].as_array() {
                                                for vs in states {
                                                    let uid = vs["user_id"].as_str().unwrap_or("").to_string();
                                                    let cid = vs["channel_id"].as_str().map(String::from);
                                                    if !uid.is_empty() {
                                                        voice_states.insert(
                                                            uid.clone(),
                                                            voice::VoiceState {
                                                                user_id: uid,
                                                                channel_id: cid,
                                                                guild_id: gid.clone(),
                                                                self_mute: vs["self_mute"].as_bool().unwrap_or(false),
                                                                self_deaf: vs["self_deaf"].as_bool().unwrap_or(false),
                                                            },
                                                        );
                                                        vs_count += 1;
                                                    }
                                                }
                                            }
                                        }
                                        if vs_count > 0 {
                                            info!("Discord: loaded {vs_count} initial voice states from READY");
                                        }
                                    }

                                    info!("Discord bot ready: {username} ({user_id})");
                                }

                                "GUILD_CREATE" => {
                                    // Parse voice_states from guild create (lazy guilds)
                                    let gid = d["id"].as_str().unwrap_or("").to_string();
                                    if let Some(states) = d["voice_states"].as_array() {
                                        for vs in states {
                                            let uid = vs["user_id"].as_str().unwrap_or("").to_string();
                                            let cid = vs["channel_id"].as_str().map(String::from);
                                            if !uid.is_empty() {
                                                voice_states.insert(
                                                    uid.clone(),
                                                    voice::VoiceState {
                                                        user_id: uid,
                                                        channel_id: cid,
                                                        guild_id: gid.clone(),
                                                        self_mute: vs["self_mute"].as_bool().unwrap_or(false),
                                                        self_deaf: vs["self_deaf"].as_bool().unwrap_or(false),
                                                    },
                                                );
                                            }
                                        }
                                        if !states.is_empty() {
                                            info!("Discord: loaded {} voice states from GUILD_CREATE for guild {gid}", states.len());
                                        }
                                    }
                                }

                                "MESSAGE_CREATE" | "MESSAGE_UPDATE" => {
                                    if let Some(msg) =
                                        parse_discord_message(d, &bot_user_id, &allowed_guilds)
                                            .await
                                    {
                                        debug!(
                                            "Discord {event_name} from {}: {:?}",
                                            msg.sender.display_name, msg.content
                                        );
                                        if tx.send(msg).await.is_err() {
                                            return;
                                        }
                                    }
                                }

                                "INTERACTION_CREATE" => {
                                    let interaction_type = d["type"].as_u64().unwrap_or(0);
                                    let interaction_id = d["id"].as_str().unwrap_or("").to_string();
                                    let interaction_token =
                                        d["token"].as_str().unwrap_or("").to_string();

                                    match interaction_type {
                                        // APPLICATION_COMMAND (slash commands) and MESSAGE_COMPONENT (buttons/selects)
                                        2 | 3 => {
                                            // Check for voice commands (/join, /leave) - handle directly
                                            let cmd_name_check = d.get("data")
                                                .and_then(|dd| dd.get("name"))
                                                .and_then(|n| n.as_str())
                                                .unwrap_or("");

                                            if cmd_name_check == "join" || cmd_name_check == "leave" {
                                                debug!("Discord voice command: /{} from user {} in guild {}", cmd_name_check, 
                                                    d.get("member").and_then(|m| m.get("user")).and_then(|u| u.get("id")).and_then(|id| id.as_str()).unwrap_or("?"),
                                                    d.get("guild_id").and_then(|g| g.as_str()).unwrap_or("?")
                                                );
                                                let guild_id_str = d.get("guild_id")
                                                    .and_then(|g| g.as_str())
                                                    .unwrap_or("")
                                                    .to_string();
                                                let user_id_str = d.get("member")
                                                    .and_then(|m| m.get("user"))
                                                    .and_then(|u| u.get("id"))
                                                    .and_then(|id| id.as_str())
                                                    .unwrap_or("")
                                                    .to_string();

                                                let ack_url = format!(
                                                    "{DISCORD_API_BASE}/interactions/{interaction_id}/{interaction_token}/callback"
                                                );

                                                if cmd_name_check == "join" {
                                                    let user_vc = voice_states.get(&user_id_str)
                                                        .and_then(|vs| vs.channel_id.clone());

                                                    if let Some(channel_id) = user_vc {
                                                        // Create voice connection entry
                                                        voice_connections.insert(
                                                            guild_id_str.clone(),
                                                            voice::VoiceConnection::new(guild_id_str.clone(), channel_id.clone()),
                                                        );

                                                        // Send gateway opcode 4 to join
                                                        let join_payload = serde_json::json!({
                                                            "op": 4,
                                                            "d": {
                                                                "guild_id": &guild_id_str,
                                                                "channel_id": &channel_id,
                                                                "self_mute": false,
                                                                "self_deaf": false
                                                            }
                                                        });
                                                        if let Ok(msg) = serde_json::to_string(&join_payload) {
                                                            let _ = ws_tx.send(
                                                                tokio_tungstenite::tungstenite::Message::Text(msg)
                                                            ).await;
                                                        }

                                                        let ack_body = serde_json::json!({
                                                            "type": 4,
                                                            "data": { "content": "Joining your voice channel..." }
                                                        });
                                                        let client = reqwest::Client::new();
                                                        tokio::spawn(async move {
                                                            let _ = client.post(&ack_url).json(&ack_body).send().await;
                                                        });
                                                    } else {
                                                        debug!("Discord /join: no voice channel found for user {}. Known voice states: {:?}",
                                                            &user_id_str,
                                                            voice_states.iter().map(|e| (e.key().clone(), e.value().channel_id.clone())).collect::<Vec<_>>()
                                                        );
                                                        let ack_body = serde_json::json!({
                                                            "type": 4,
                                                            "data": { "content": "You need to be in a voice channel first! (Try leaving and rejoining your voice channel)" }
                                                        });
                                                        let client = reqwest::Client::new();
                                                        tokio::spawn(async move {
                                                            let _ = client.post(&ack_url).json(&ack_body).send().await;
                                                        });
                                                    }
                                                } else {
                                                    // /leave command
                                                    if let Some(mut mgr) = voice_managers.get_mut(&guild_id_str) {
                                                        mgr.disconnect();
                                                    }
                                                    voice_managers.remove(&guild_id_str);
                                                    voice_connections.remove(&guild_id_str);

                                                    // Send opcode 4 with null channel
                                                    let leave_payload = serde_json::json!({
                                                        "op": 4,
                                                        "d": {
                                                            "guild_id": &guild_id_str,
                                                            "channel_id": serde_json::Value::Null,
                                                            "self_mute": false,
                                                            "self_deaf": false
                                                        }
                                                    });
                                                    if let Ok(msg) = serde_json::to_string(&leave_payload) {
                                                        let _ = ws_tx.send(
                                                            tokio_tungstenite::tungstenite::Message::Text(msg)
                                                        ).await;
                                                    }

                                                    let ack_body = serde_json::json!({
                                                        "type": 4,
                                                        "data": { "content": "Left voice channel." }
                                                    });
                                                    let client = reqwest::Client::new();
                                                    tokio::spawn(async move {
                                                        let _ = client.post(&ack_url).json(&ack_body).send().await;
                                                    });
                                                }
                                                // Don't send to bridge - voice commands handled internally
                                            } else if let Some(msg) =
                                                parse_interaction(d, &bot_user_id, interaction_type)
                                                    .await
                                            {
                                                // ACK the interaction immediately via REST (fire and forget)
                                                let ack_url = format!(
                                                    "{DISCORD_API_BASE}/interactions/{interaction_id}/{interaction_token}/callback"
                                                );
                                                let ack_type =
                                                    if interaction_type == 3 { 6 } else { 5 }; // 6=DEFERRED_UPDATE, 5=DEFERRED_CHANNEL_MESSAGE
                                                let ack_body = serde_json::json!({ "type": ack_type });
                                                let client = reqwest::Client::new();
                                                tokio::spawn(async move {
                                                    let _ = client.post(&ack_url).json(&ack_body).send().await;
                                                });

                                                if tx.send(msg).await.is_err() {
                                                    return;
                                                }
                                            }
                                        }
                                        // APPLICATION_COMMAND_AUTOCOMPLETE
                                        4 => {
                                            debug!(
                                                "Discord: autocomplete interaction (not yet implemented)"
                                            );
                                        }
                                        // MODAL_SUBMIT
                                        5 => {
                                            if let Some(msg) =
                                                parse_modal_submit(d, &bot_user_id).await
                                            {
                                                let ack_url = format!(
                                                    "{DISCORD_API_BASE}/interactions/{interaction_id}/{interaction_token}/callback"
                                                );
                                                let ack_body = serde_json::json!({ "type": 6 });
                                                let client = reqwest::Client::new();
                                                tokio::spawn(async move {
                                                    let _ = client.post(&ack_url).json(&ack_body).send().await;
                                                });
                                                if tx.send(msg).await.is_err() {
                                                    return;
                                                }
                                            }
                                        }
                                        _ => {
                                            debug!(
                                                "Discord: unknown interaction type {interaction_type}"
                                            );
                                        }
                                    }
                                }

                                "VOICE_STATE_UPDATE" => {
                                    let guild_id = d["guild_id"].as_str().unwrap_or("").to_string();
                                    let user_id = d["user_id"].as_str().unwrap_or("").to_string();
                                    let channel_id = d["channel_id"].as_str().map(String::from);
                                    let session_id = d["session_id"].as_str().map(String::from);

                                    // Check if this is the bot's voice state
                                    let is_bot = if let Some(ref bid) = *bot_user_id.read().await
                                    {
                                        &user_id == bid
                                    } else {
                                        false
                                    };

                                    if is_bot {
                                        if let Some(ref sid) = session_id {
                                            if let Some(mut conn) = voice_connections.get_mut(&guild_id) {
                                                conn.session_id = Some(sid.clone());
                                                info!(
                                                    "Discord: bot voice session updated for guild {guild_id}"
                                                );
                                            }
                                        }
                                    }

                                    // Track all users' voice states
                                    voice_states.insert(
                                        user_id.clone(),
                                        voice::VoiceState {
                                            user_id,
                                            channel_id,
                                            guild_id,
                                            self_mute: d["self_mute"].as_bool().unwrap_or(false),
                                            self_deaf: d["self_deaf"].as_bool().unwrap_or(false),
                                        },
                                    );
                                }

                                "VOICE_SERVER_UPDATE" => {
                                    let guild_id = d["guild_id"].as_str().unwrap_or("").to_string();
                                    let endpoint = d["endpoint"].as_str().map(String::from);
                                    let voice_token = d["token"].as_str().map(String::from);

                                    if let Some(mut conn) = voice_connections.get_mut(&guild_id) {
                                        conn.endpoint = endpoint;
                                        conn.token = voice_token;
                                        if conn.is_ready() {
                                            info!(
                                                "Discord: voice connection ready for guild {guild_id}, endpoint: {:?}",
                                                conn.endpoint
                                            );

                                            // Get bot user ID for Songbird ConnectionInfo
                                            let bot_id_str = bot_user_id.read().await.clone().unwrap_or_default();
                                            let bot_id_num: u64 = bot_id_str.parse().unwrap_or(0);

                                            if bot_id_num > 0 {
                                                let conn_snapshot = conn.clone();
                                                let vm = voice_managers.clone();
                                                let client_clone = reqwest::Client::new();

                                                tokio::spawn(async move {
                                                    let mut manager = voice::SongbirdVoiceManager::new();
                                                    match manager.connect(&conn_snapshot, bot_id_num).await {
                                                        Ok(()) => {
                                                            // Set up audio receive pipeline
                                                            let (audio_tx, mut audio_rx) =
                                                                tokio::sync::mpsc::unbounded_channel::<voice::AudioCompleteEvent>();
                                                            let receiver = voice::AudioReceiver::new(audio_tx);

                                                            // Register all required receive events with cloned receivers
                                                            use songbird::events::{Event as SbEvent, CoreEvent as SbCore};
                                                            manager.register_event(
                                                                SbEvent::Core(SbCore::VoiceTick),
                                                                receiver.clone(),
                                                            );
                                                            manager.register_event(
                                                                SbEvent::Core(SbCore::SpeakingStateUpdate),
                                                                receiver.clone(),
                                                            );
                                                            manager.register_event(
                                                                SbEvent::Core(SbCore::ClientDisconnect),
                                                                receiver,
                                                            );

                                                            let gid = conn_snapshot.guild_id.clone();
                                                            vm.insert(gid.clone(), manager);

                                                            // Spawn audio processing pipeline
                                                            let vm2 = vm.clone();
                                                            let api_base = std::env::var("OPENFANG_URL").unwrap_or_else(|_| "http://127.0.0.1:4200".to_string());
                                                            let api_key = std::env::var("OPENFANG_API_KEY").unwrap_or_else(|_| "ofk-yog-2026".to_string());
                                                            let default_agent = std::env::var("OPENFANG_DEFAULT_AGENT").unwrap_or_else(|_| "assistant".to_string());
                                                            tokio::spawn(async move {
                                                                while let Some(evt) = audio_rx.recv().await {
                                                                    let ts = std::time::SystemTime::now()
                                                                        .duration_since(std::time::UNIX_EPOCH)
                                                                        .unwrap()
                                                                        .as_millis();
                                                                    let uid = evt.user_id.unwrap_or(evt.ssrc as u64);
                                                                    let wav_path = format!("/tmp/openfang_voice_{uid}_{ts}.wav");

                                                                    if let Err(e) = voice::save_pcm_to_wav(&wav_path, &evt.samples) {
                                                                        error!("Voice: failed to save WAV: {e}");
                                                                        continue;
                                                                    }
                                                                    info!("Voice: saved {:.1}s audio from user {uid} to {wav_path}",
                                                                        evt.samples.len() as f64 / 48000.0 / 2.0);

                                                                    // 1) Transcribe locally via whisper-cli
                                                                    let whisper_model = std::env::var("WHISPER_MODEL")
                                                                        .unwrap_or_else(|_| "/home/yog/.openfang/models/ggml-base.en.bin".to_string());
                                                                    let whisper_bin = std::env::var("WHISPER_CLI")
                                                                        .unwrap_or_else(|_| "/usr/local/bin/whisper-cli".to_string());
                                                                    let ncpu = std::thread::available_parallelism()
                                                                        .map(|n| n.get()).unwrap_or(4).min(8);
                                                                    let stt_output = tokio::process::Command::new(&whisper_bin)
                                                                        .arg("-m").arg(&whisper_model)
                                                                        .arg("-f").arg(&wav_path)
                                                                        .arg("--no-timestamps")
                                                                        .arg("-np")
                                                                        .arg("-t").arg(ncpu.to_string())
                                                                        .output()
                                                                        .await;
                                                                    let transcript = match stt_output {
                                                                        Ok(o) if o.status.success() => {
                                                                            String::from_utf8_lossy(&o.stdout).trim().to_string()
                                                                        }
                                                                        Ok(o) => {
                                                                            error!("Voice STT: whisper-cli exit {}: {}",
                                                                                o.status, String::from_utf8_lossy(&o.stderr));
                                                                            String::new()
                                                                        }
                                                                        Err(e) => {
                                                                            error!("Voice STT failed to run whisper-cli: {e}");
                                                                            String::new()
                                                                        }
                                                                    };

                                                                    if transcript.trim().is_empty() {
                                                                        debug!("Voice: empty transcript, skipping");
                                                                        let _ = std::fs::remove_file(&wav_path);
                                                                        continue;
                                                                    }
                                                                    info!("Voice STT: {transcript}");

                                                                    // 2) Send to default agent
                                                                    let msg_body = serde_json::json!({
                                                                        "message": &transcript
                                                                    });
                                                                    let agent_resp = client_clone
                                                                        .post(&format!("{api_base}/api/agents/{default_agent}/message"))
                                                                        .header("Authorization", &format!("Bearer {api_key}"))
                                                                        .json(&msg_body)
                                                                        .send()
                                                                        .await;

                                                                    let reply = match agent_resp {
                                                                        Ok(r) => {
                                                                            let text = r.text().await.unwrap_or_default();
                                                                            let v: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
                                                                            v["response"]
                                                                                .as_str()
                                                                                .or(v["content"].as_str())
                                                                                .unwrap_or("")
                                                                                .to_string()
                                                                        }
                                                                        Err(e) => {
                                                                            error!("Voice agent call failed: {e}");
                                                                            String::new()
                                                                        }
                                                                    };

                                                                    if reply.trim().is_empty() {
                                                                        warn!("Voice: empty agent reply");
                                                                        let _ = std::fs::remove_file(&wav_path);
                                                                        continue;
                                                                    }
                                                                    info!("Voice agent reply: {reply}");

                                                                    // 3) TTS via local Piper (fast, zero-cost)
                                                                    let piper_bin = std::env::var("PIPER_BIN")
                                                                        .unwrap_or_else(|_| "/usr/local/bin/piper".to_string());
                                                                    let piper_model = std::env::var("PIPER_MODEL")
                                                                        .unwrap_or_else(|_| "/home/yog/.openfang/models/piper/en_US-amy-medium.onnx".to_string());
                                                                    let tts_path = format!("/tmp/openfang_tts_{ts}.wav");
                                                                    let tts_output = tokio::process::Command::new(&piper_bin)
                                                                        .arg("-m").arg(&piper_model)
                                                                        .arg("-f").arg(&tts_path)
                                                                        .stdin(std::process::Stdio::piped())
                                                                        .stdout(std::process::Stdio::null())
                                                                        .stderr(std::process::Stdio::piped())
                                                                        .spawn();
                                                                    match tts_output {
                                                                        Ok(mut child) => {
                                                                            // Write reply text to piper's stdin
                                                                            if let Some(mut stdin) = child.stdin.take() {
                                                                                use tokio::io::AsyncWriteExt;
                                                                                let _ = stdin.write_all(reply.as_bytes()).await;
                                                                                drop(stdin);
                                                                            }
                                                                            match child.wait().await {
                                                                                Ok(status) if status.success() => {
                                                                                    info!("Voice TTS: piper generated {tts_path}");
                                                                                    if let Some(mut mgr) = vm2.get_mut(&gid) {
                                                                                        mgr.play_file(&tts_path);
                                                                                    }
                                                                                    // Cleanup after playback
                                                                                    let p = tts_path.clone();
                                                                                    tokio::spawn(async move {
                                                                                        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                                                                                        let _ = std::fs::remove_file(&p);
                                                                                    });
                                                                                }
                                                                                Ok(status) => {
                                                                                    error!("Voice TTS: piper exit {status}");
                                                                                }
                                                                                Err(e) => {
                                                                                    error!("Voice TTS: piper wait failed: {e}");
                                                                                }
                                                                            }
                                                                        }
                                                                        Err(e) => {
                                                                            error!("Voice TTS: failed to spawn piper: {e}");
                                                                        }
                                                                    }

                                                                    // Cleanup temp WAV
                                                                    let _ = std::fs::remove_file(&wav_path);
                                                                }
                                                                info!("Voice: audio pipeline exited for guild {gid}");
                                                            });
                                                        }
                                                        Err(e) => {
                                                            error!("Discord: Songbird voice connect failed: {e}");
                                                        }
                                                    }
                                                });
                                            }
                                        }
                                    }
                                }

                                "RESUMED" => {
                                    info!("Discord session resumed successfully");
                                }

                                _ => {
                                    debug!("Discord event: {event_name}");
                                }
                            }
                        }

                        opcode::HEARTBEAT => {
                            // Server requests immediate heartbeat
                            let seq = *sequence.read().await;
                            let hb = serde_json::json!({ "op": opcode::HEARTBEAT, "d": seq });
                            let _ = ws_tx
                                .send(tokio_tungstenite::tungstenite::Message::Text(
                                    serde_json::to_string(&hb).unwrap(),
                                ))
                                .await;
                        }

                        opcode::HEARTBEAT_ACK => {
                            debug!("Discord heartbeat ACK received");
                            awaiting_heartbeat_ack = false;
                        }

                        opcode::RECONNECT => {
                            info!("Discord: server requested reconnect");
                            break 'inner true;
                        }

                        opcode::INVALID_SESSION => {
                            let resumable = payload["d"].as_bool().unwrap_or(false);
                            if resumable {
                                info!("Discord: invalid session (resumable)");
                            } else {
                                info!("Discord: invalid session (not resumable), clearing session");
                                *session_id_store.write().await = None;
                                *sequence.write().await = None;
                            }
                            break 'inner true;
                        }

                        _ => {
                            debug!("Discord: unknown opcode {op}");
                        }
                    }
                };

                if !should_reconnect || *shutdown.borrow() {
                    break;
                }

                // Try resume URL if available
                if let Some(ref url) = *resume_url_store.read().await {
                    connect_url = format!("{url}/?v=10&encoding=json");
                }

                warn!("Discord: reconnecting in {backoff:?}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }

            info!("Discord gateway loop stopped");
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Box::pin(stream))
    }

    async fn send(
        &self,
        user: &ChannelUser,
        content: ChannelContent,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // platform_id is the channel_id for Discord
        let channel_id = &user.platform_id;
        match content {
            ChannelContent::Text(text) => {
                self.api_send_message(channel_id, &text).await?;
            }
            ChannelContent::Image { url, caption } => {
                self.api_send_image_embed(channel_id, &url, caption.as_deref())
                    .await?;
            }
            ChannelContent::File { url, filename } => {
                // Download the file, then upload as attachment
                match self.client.get(&url).send().await {
                    Ok(resp) => {
                        let bytes = resp.bytes().await?.to_vec();
                        self.api_send_file(channel_id, bytes, &filename, Some(&filename))
                            .await?;
                    }
                    Err(e) => {
                        warn!("Discord: failed to download file {url}: {e}");
                        self.api_send_message(
                            channel_id,
                            &format!("📎 [{filename}]({url})"),
                        )
                        .await?;
                    }
                }
            }
            ChannelContent::Voice {
                url,
                duration_seconds,
            } => {
                match self.client.get(&url).send().await {
                    Ok(resp) => {
                        let bytes = resp.bytes().await?.to_vec();
                        self.api_send_file(
                            channel_id,
                            bytes,
                            "voice_message.ogg",
                            Some(&format!("🎤 Voice message ({duration_seconds}s)")),
                        )
                        .await?;
                    }
                    Err(e) => {
                        warn!("Discord: failed to download voice {url}: {e}");
                        self.api_send_message(
                            channel_id,
                            &format!("🎤 Voice message ({duration_seconds}s)"),
                        )
                        .await?;
                    }
                }
            }
            ChannelContent::Location { lat, lon } => {
                let embed = discord_types::DiscordEmbed {
                    title: Some("📍 Location".to_string()),
                    description: Some(format!("{lat}, {lon}")),
                    url: Some(format!("https://www.google.com/maps?q={lat},{lon}")),
                    color: Some(0x2ECC71),
                    ..Default::default()
                };
                self.api_send_embed(channel_id, None, &embed).await?;
            }
            ChannelContent::Interactive { message, components } => {
                self.api_send_with_components(channel_id, &message, &components)
                    .await?;
            }
            ChannelContent::Rich { blocks, fallback_text } => {
                self.send_rich_blocks(channel_id, &blocks, &fallback_text)
                    .await?;
            }
            ChannelContent::Command { .. } => {
                // Commands are handled upstream by the bridge
            }
        }
        Ok(())
    }

    async fn send_reaction(
        &self,
        user: &ChannelUser,
        message_id: &str,
        reaction: &LifecycleReaction,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let channel_id = &user.platform_id;
        if reaction.remove_previous {
            // Remove all our reactions first
            let _ = self
                .api_remove_all_own_reactions(channel_id, message_id)
                .await;
        }
        self.api_add_reaction(channel_id, message_id, &reaction.emoji)
            .await
    }

    async fn send_typing(&self, user: &ChannelUser) -> Result<(), Box<dyn std::error::Error>> {
        self.api_send_typing(&user.platform_id).await
    }

    async fn stop(&self) -> Result<(), Box<dyn std::error::Error>> {
        let _ = self.shutdown_tx.send(true);
        Ok(())
    }
}

// ============= Helper Functions =============

/// URL-encode an emoji for Discord reaction endpoints.
fn url_encode_emoji(emoji: &str) -> String {
    emoji
        .bytes()
        .map(|b| format!("%{:02X}", b))
        .collect()
}

/// Register slash commands with Discord (Phase 3).
async fn register_slash_commands(
    client: &reqwest::Client,
    token: &str,
    app_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("{DISCORD_API_BASE}/applications/{app_id}/commands");
    let commands = serde_json::json!([
        {
            "name": "ask",
            "description": "Ask the AI agent a question",
            "options": [{ "name": "prompt", "description": "Your question", "type": 3, "required": true }]
        },
        {
            "name": "agent",
            "description": "Route to a specific agent",
            "options": [
                { "name": "name", "description": "Agent name", "type": 3, "required": true },
                { "name": "prompt", "description": "Your message", "type": 3, "required": true }
            ]
        },
        {
            "name": "research",
            "description": "Start a deep research task",
            "options": [{ "name": "query", "description": "Research topic", "type": 3, "required": true }]
        },
        {
            "name": "code",
            "description": "Generate or analyze code",
            "options": [{ "name": "task", "description": "What to code", "type": 3, "required": true }]
        },
        {
            "name": "status",
            "description": "Show agent status and system info"
        },
        {
            "name": "model",
            "description": "Show or switch the current model",
            "options": [{ "name": "name", "description": "Model name (leave empty to show current)", "type": 3, "required": false }]
        },
        {
            "name": "reset",
            "description": "Reset conversation context"
        },
        {
            "name": "join",
            "description": "Join your current voice channel"
        },
        {
            "name": "leave",
            "description": "Leave the voice channel"
        }
    ]);

    let resp = client
        .put(&url)
        .header("Authorization", format!("Bot {token}"))
        .json(&commands)
        .send()
        .await?;

    if resp.status().is_success() {
        info!("Discord: registered slash commands");
    } else {
        let body = resp.text().await.unwrap_or_default();
        warn!("Discord: failed to register slash commands: {body}");
    }
    Ok(())
}

/// Parse a Discord interaction (slash command or component click) into a ChannelMessage (Phase 2-3).
async fn parse_interaction(
    d: &serde_json::Value,
    bot_user_id: &Arc<RwLock<Option<String>>>,
    interaction_type: u64,
) -> Option<ChannelMessage> {
    let channel_id = d["channel_id"].as_str()?;
    let user = d
        .get("member")
        .and_then(|m| m.get("user"))
        .or_else(|| d.get("user"))?;
    let author_id = user["id"].as_str()?;
    let username = user["username"].as_str().unwrap_or("Unknown");

    // Don't process our own interactions
    if let Some(ref bid) = *bot_user_id.read().await {
        if author_id == bid {
            return None;
        }
    }

    let (name, args) = match interaction_type {
        2 => {
            // APPLICATION_COMMAND
            let data = d.get("data")?;
            let cmd_name = data["name"].as_str()?.to_string();
            let options = data
                .get("options")
                .and_then(|o| o.as_array())
                .cloned()
                .unwrap_or_default();
            let cmd_args: Vec<String> = options
                .iter()
                .filter_map(|opt| opt["value"].as_str().map(|s| s.to_string()))
                .collect();
            (cmd_name, cmd_args)
        }
        3 => {
            // MESSAGE_COMPONENT (button click or select menu)
            let data = d.get("data")?;
            let custom_id = data["custom_id"].as_str()?.to_string();
            let values: Vec<String> = data
                .get("values")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            (custom_id, values)
        }
        _ => return None,
    };

    let message_id = d["id"].as_str().unwrap_or("0");

    Some(ChannelMessage {
        channel: ChannelType::Discord,
        platform_message_id: message_id.to_string(),
        sender: ChannelUser {
            platform_id: channel_id.to_string(),
            display_name: username.to_string(),
            openfang_user: None,
        },
        content: ChannelContent::Command { name, args },
        target_agent: None,
        timestamp: chrono::Utc::now(),
        is_group: d.get("guild_id").is_some(),
        thread_id: None,
        metadata: {
            let mut m = HashMap::new();
            m.insert(
                "interaction_id".to_string(),
                serde_json::json!(d["id"].as_str().unwrap_or("")),
            );
            m.insert(
                "interaction_token".to_string(),
                serde_json::json!(d["token"].as_str().unwrap_or("")),
            );
            m
        },
    })
}

/// Parse a modal submit interaction (Phase 2).
async fn parse_modal_submit(
    d: &serde_json::Value,
    bot_user_id: &Arc<RwLock<Option<String>>>,
) -> Option<ChannelMessage> {
    let channel_id = d["channel_id"].as_str()?;
    let user = d
        .get("member")
        .and_then(|m| m.get("user"))
        .or_else(|| d.get("user"))?;
    let author_id = user["id"].as_str()?;
    let username = user["username"].as_str().unwrap_or("Unknown");

    if let Some(ref bid) = *bot_user_id.read().await {
        if author_id == bid {
            return None;
        }
    }

    let data = d.get("data")?;
    let custom_id = data["custom_id"].as_str()?.to_string();

    // Extract text input values from modal components
    let mut values = Vec::new();
    if let Some(components) = data.get("components").and_then(|c| c.as_array()) {
        for row in components {
            if let Some(inner) = row.get("components").and_then(|c| c.as_array()) {
                for comp in inner {
                    if let Some(val) = comp["value"].as_str() {
                        values.push(val.to_string());
                    }
                }
            }
        }
    }

    Some(ChannelMessage {
        channel: ChannelType::Discord,
        platform_message_id: d["id"].as_str().unwrap_or("0").to_string(),
        sender: ChannelUser {
            platform_id: channel_id.to_string(),
            display_name: username.to_string(),
            openfang_user: None,
        },
        content: ChannelContent::Command {
            name: custom_id,
            args: values,
        },
        target_agent: None,
        timestamp: chrono::Utc::now(),
        is_group: d.get("guild_id").is_some(),
        thread_id: None,
        metadata: HashMap::new(),
    })
}

/// Parse a Discord MESSAGE_CREATE or MESSAGE_UPDATE payload into a `ChannelMessage`.
async fn parse_discord_message(
    d: &serde_json::Value,
    bot_user_id: &Arc<RwLock<Option<String>>>,
    allowed_guilds: &[String],
) -> Option<ChannelMessage> {
    let author = d.get("author")?;
    let author_id = author["id"].as_str()?;

    // Filter out bot's own messages
    if let Some(ref bid) = *bot_user_id.read().await {
        if author_id == bid {
            return None;
        }
    }

    // Filter out other bots
    if author["bot"].as_bool() == Some(true) {
        return None;
    }

    // Filter by allowed guilds
    if !allowed_guilds.is_empty() {
        if let Some(guild_id) = d["guild_id"].as_str() {
            if !allowed_guilds.iter().any(|g| g == guild_id) {
                return None;
            }
        }
    }

    let content_text = d["content"].as_str().unwrap_or("");

    // Parse attachments (files, images sent via Discord)
    let mut attachment_lines: Vec<String> = Vec::new();
    if let Some(attachments) = d.get("attachments").and_then(|a| a.as_array()) {
        for att in attachments {
            let filename = att["filename"].as_str().unwrap_or("file");
            let url = att["url"].as_str().unwrap_or("");
            let content_type = att["content_type"].as_str().unwrap_or("");
            let size = att["size"].as_u64().unwrap_or(0);
            if !url.is_empty() {
                if content_type.starts_with("image/") {
                    attachment_lines.push(format!("[Attached image: {filename}]({url})"));
                } else {
                    let size_str = if size > 1_000_000 {
                        format!("{:.1}MB", size as f64 / 1_000_000.0)
                    } else if size > 1000 {
                        format!("{:.0}KB", size as f64 / 1000.0)
                    } else {
                        format!("{size}B")
                    };
                    attachment_lines.push(
                        format!("[Attached file: {filename} ({size_str})]({url})")
                    );
                }
            }
        }
    }

    // Skip if both text content and attachments are empty
    if content_text.is_empty() && attachment_lines.is_empty() {
        return None;
    }

    // Combine text with attachment info
    let combined_text = if attachment_lines.is_empty() {
        content_text.to_string()
    } else if content_text.is_empty() {
        attachment_lines.join("\n")
    } else {
        format!("{}\n\n{}", content_text, attachment_lines.join("\n"))
    };
    let content_text = &combined_text;

    let channel_id = d["channel_id"].as_str()?;
    let message_id = d["id"].as_str().unwrap_or("0");
    let username = author["username"].as_str().unwrap_or("Unknown");
    let discriminator = author["discriminator"].as_str().unwrap_or("0000");
    let display_name = if discriminator == "0" {
        username.to_string()
    } else {
        format!("{username}#{discriminator}")
    };

    let timestamp = d["timestamp"]
        .as_str()
        .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(chrono::Utc::now);

    // Parse commands (messages starting with /)
    let content = if content_text.starts_with('/') {
        let parts: Vec<&str> = content_text.splitn(2, ' ').collect();
        let cmd_name = &parts[0][1..];
        let args = if parts.len() > 1 {
            parts[1]
                .split_whitespace()
                .map(String::from)
                .collect()
        } else {
            vec![]
        };
        ChannelContent::Command {
            name: cmd_name.to_string(),
            args,
        }
    } else {
        ChannelContent::Text(content_text.to_string())
    };

    Some(ChannelMessage {
        channel: ChannelType::Discord,
        platform_message_id: message_id.to_string(),
        sender: ChannelUser {
            platform_id: channel_id.to_string(),
            display_name,
            openfang_user: None,
        },
        content,
        target_agent: None,
        timestamp,
        is_group: true,
        thread_id: None,
        metadata: HashMap::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_parse_discord_message_basic() {
        let bot_id = Arc::new(RwLock::new(Some("bot123".to_string())));
        let d = serde_json::json!({
            "id": "msg1",
            "channel_id": "ch1",
            "content": "Hello agent!",
            "author": {
                "id": "user456",
                "username": "alice",
                "discriminator": "0",
                "bot": false
            },
            "timestamp": "2024-01-01T00:00:00+00:00"
        });

        let msg = parse_discord_message(&d, &bot_id, &[])
            .await
            .unwrap();
        assert_eq!(msg.channel, ChannelType::Discord);
        assert_eq!(msg.sender.display_name, "alice");
        assert_eq!(msg.sender.platform_id, "ch1");
        assert!(matches!(msg.content, ChannelContent::Text(ref t) if t == "Hello agent!"));
    }

    #[tokio::test]
    async fn test_parse_discord_message_filters_bot() {
        let bot_id = Arc::new(RwLock::new(Some("bot123".to_string())));
        let d = serde_json::json!({
            "id": "msg1",
            "channel_id": "ch1",
            "content": "My own message",
            "author": {
                "id": "bot123",
                "username": "openfang",
                "discriminator": "0"
            },
            "timestamp": "2024-01-01T00:00:00+00:00"
        });

        let msg = parse_discord_message(&d, &bot_id, &[]).await;
        assert!(msg.is_none());
    }

    #[tokio::test]
    async fn test_parse_discord_message_filters_other_bots() {
        let bot_id = Arc::new(RwLock::new(Some("bot123".to_string())));
        let d = serde_json::json!({
            "id": "msg1",
            "channel_id": "ch1",
            "content": "Bot message",
            "author": {
                "id": "other_bot",
                "username": "somebot",
                "discriminator": "0",
                "bot": true
            },
            "timestamp": "2024-01-01T00:00:00+00:00"
        });

        let msg = parse_discord_message(&d, &bot_id, &[]).await;
        assert!(msg.is_none());
    }

    #[tokio::test]
    async fn test_parse_discord_message_guild_filter() {
        let bot_id = Arc::new(RwLock::new(Some("bot123".to_string())));
        let d = serde_json::json!({
            "id": "msg1",
            "channel_id": "ch1",
            "guild_id": "999",
            "content": "Hello",
            "author": {
                "id": "user1",
                "username": "bob",
                "discriminator": "0"
            },
            "timestamp": "2024-01-01T00:00:00+00:00"
        });

        // Not in allowed guilds
        let msg = parse_discord_message(&d, &bot_id, &["111".into(), "222".into()]).await;
        assert!(msg.is_none());

        // In allowed guilds
        let msg = parse_discord_message(&d, &bot_id, &["999".into()]).await;
        assert!(msg.is_some());
    }

    #[tokio::test]
    async fn test_parse_discord_command() {
        let bot_id = Arc::new(RwLock::new(None));
        let d = serde_json::json!({
            "id": "msg1",
            "channel_id": "ch1",
            "content": "/agent hello-world",
            "author": {
                "id": "user1",
                "username": "alice",
                "discriminator": "0"
            },
            "timestamp": "2024-01-01T00:00:00+00:00"
        });

        let msg = parse_discord_message(&d, &bot_id, &[])
            .await
            .unwrap();
        match &msg.content {
            ChannelContent::Command { name, args } => {
                assert_eq!(name, "agent");
                assert_eq!(args, &["hello-world"]);
            }
            other => panic!("Expected Command, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_parse_discord_empty_content() {
        let bot_id = Arc::new(RwLock::new(None));
        let d = serde_json::json!({
            "id": "msg1",
            "channel_id": "ch1",
            "content": "",
            "author": {
                "id": "user1",
                "username": "alice",
                "discriminator": "0"
            },
            "timestamp": "2024-01-01T00:00:00+00:00"
        });

        let msg = parse_discord_message(&d, &bot_id, &[]).await;
        assert!(msg.is_none());
    }

    #[tokio::test]
    async fn test_parse_discord_discriminator() {
        let bot_id = Arc::new(RwLock::new(None));
        let d = serde_json::json!({
            "id": "msg1",
            "channel_id": "ch1",
            "content": "Hi",
            "author": {
                "id": "user1",
                "username": "alice",
                "discriminator": "1234"
            },
            "timestamp": "2024-01-01T00:00:00+00:00"
        });

        let msg = parse_discord_message(&d, &bot_id, &[])
            .await
            .unwrap();
        assert_eq!(msg.sender.display_name, "alice#1234");
    }

    #[tokio::test]
    async fn test_parse_discord_message_update() {
        let bot_id = Arc::new(RwLock::new(Some("bot123".to_string())));
        let d = serde_json::json!({
            "id": "msg1",
            "channel_id": "ch1",
            "content": "Edited message content",
            "author": {
                "id": "user456",
                "username": "alice",
                "discriminator": "0",
                "bot": false
            },
            "timestamp": "2024-01-01T00:00:00+00:00",
            "edited_timestamp": "2024-01-01T00:01:00+00:00"
        });

        // MESSAGE_UPDATE uses the same parse function as MESSAGE_CREATE
        let msg = parse_discord_message(&d, &bot_id, &[])
            .await
            .unwrap();
        assert_eq!(msg.channel, ChannelType::Discord);
        assert!(
            matches!(msg.content, ChannelContent::Text(ref t) if t == "Edited message content")
        );
    }


    #[tokio::test]
    async fn test_parse_interaction_slash_command() {
        let bot_id = Arc::new(RwLock::new(Some("bot123".to_string())));
        let d = serde_json::json!({
            "id": "interaction1",
            "type": 2,
            "channel_id": "ch1",
            "token": "token123",
            "user": {
                "id": "user456",
                "username": "alice",
            },
            "data": {
                "name": "ask",
                "options": [
                    { "name": "prompt", "value": "What is AI?", "type": 3 }
                ]
            }
        });

        let msg = parse_interaction(&d, &bot_id, 2).await.unwrap();
        assert_eq!(msg.channel, ChannelType::Discord);
        match &msg.content {
            ChannelContent::Command { name, args } => {
                assert_eq!(name, "ask");
                assert_eq!(args, &["What is AI?"]);
            }
            other => panic!("Expected Command, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_parse_interaction_button_click() {
        let bot_id = Arc::new(RwLock::new(Some("bot123".to_string())));
        let d = serde_json::json!({
            "id": "interaction1",
            "type": 3,
            "channel_id": "ch1",
            "token": "token123",
            "user": {
                "id": "user456",
                "username": "alice",
            },
            "data": {
                "custom_id": "button_confirm",
                "component_type": 2
            }
        });

        let msg = parse_interaction(&d, &bot_id, 3).await.unwrap();
        match &msg.content {
            ChannelContent::Command { name, args } => {
                assert_eq!(name, "button_confirm");
                assert!(args.is_empty());
            }
            other => panic!("Expected Command, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_parse_modal_submit() {
        let bot_id = Arc::new(RwLock::new(Some("bot123".to_string())));
        let d = serde_json::json!({
            "id": "interaction1",
            "type": 5,
            "channel_id": "ch1",
            "user": {
                "id": "user456",
                "username": "alice",
            },
            "data": {
                "custom_id": "feedback_form",
                "components": [
                    {
                        "type": 1,
                        "components": [
                            {
                                "type": 4,
                                "custom_id": "feedback_text",
                                "value": "Great bot!"
                            }
                        ]
                    }
                ]
            }
        });

        let msg = parse_modal_submit(&d, &bot_id).await.unwrap();
        match &msg.content {
            ChannelContent::Command { name, args } => {
                assert_eq!(name, "feedback_form");
                assert_eq!(args, &["Great bot!"]);
            }
            other => panic!("Expected Command, got {other:?}"),
        }
    }

    #[test]
    fn test_url_encode_emoji() {
        assert_eq!(url_encode_emoji("😊"), "%F0%9F%98%8A");
        assert_eq!(url_encode_emoji("✅"), "%E2%9C%85");
    }

    #[test]
    fn test_discord_adapter_creation() {
        let adapter = DiscordAdapter::new(
            "test-token".to_string(),
            vec!["123".to_string(), "456".to_string()],
            37376,
        );
        assert_eq!(adapter.name(), "discord");
        assert_eq!(adapter.channel_type(), ChannelType::Discord);
    }
}

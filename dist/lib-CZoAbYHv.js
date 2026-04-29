import "./chunk-ZRW5x4aP.js";
import { EventEmitter } from "node:events";
import * as tikTokSchema from "tiktok-live-proto/v2";
import { ControlAction, EnvelopeBusinessType, HeartbeatMessage, WebcastImEnterRoomMessage, WebcastPushFrame } from "tiktok-live-proto/v2";
import * as zlib from "node:zlib";
import * as util from "node:util";
import { base64Encode } from "@bufbuild/protobuf/wire";
import { WebSocket } from "ws";
import got from "got";
import EulerStreamApiClient, { WebcastFetchPlatform } from "@eulerstream/euler-api-sdk";
import { isIP } from "node:net";
//#region src/types/errors.ts
var ConnectError = class extends Error {
	constructor(message) {
		super(message);
	}
};
var InvalidUniqueIdError = class extends Error {};
var InvalidResponseError = class extends Error {
	constructor(config, ...args) {
		super(`[${config.routeId}] ${args.join(" ")}`);
		this.config = config;
	}
};
var InvalidResponseCompositeError = class extends Error {
	constructor(config, ...args) {
		super(...args);
		this.config = config;
	}
};
var InvalidRequestError = class extends Error {
	constructor(config, ...args) {
		super(`[${config.routeId}] ${args.join(" ")}`);
		this.config = config;
	}
};
var AlreadyConnectingError = class extends ConnectError {};
var AlreadyConnectedError = class extends ConnectError {};
var UserOfflineError = class extends ConnectError {};
var ConnectTimeoutError = class extends ConnectError {};
var InvalidSchemaNameError = class extends Error {};
var SchemaDecodeError = class extends Error {};
var TikTokLiveError = class extends Error {
	constructor(message) {
		super(message);
		this.name = this.constructor.name;
	}
};
let ErrorReason = /* @__PURE__ */ function(ErrorReason) {
	ErrorReason["RATE_LIMIT"] = "Rate Limited";
	ErrorReason["CONNECT_ERROR"] = "Connect Error";
	ErrorReason["EMPTY_PAYLOAD"] = "Empty Payload";
	ErrorReason["SIGN_NOT_200"] = "Sign Error";
	ErrorReason["EMPTY_COOKIES"] = "Empty Cookies";
	ErrorReason["PREMIUM_FEATURE"] = "Premium Feature";
	ErrorReason["AUTHENTICATED_WS"] = "Authenticated WS";
	return ErrorReason;
}({});
var SignAPIError = class extends TikTokLiveError {
	reason;
	requestId;
	agentId;
	constructor(reason, requestId, agentId, ...args) {
		super([`[${reason}]`, ...args.filter((a) => a !== void 0).map((a) => a instanceof Error ? a.message : a)].join(" "));
		this.reason = reason;
		this.requestId = requestId;
		this.agentId = agentId;
	}
	static formatSignServerMessage(message) {
		message = message.trim();
		const msgLen = message.length;
		const headerLen = Math.floor((msgLen - 19) / 2);
		const paddingLen = (msgLen - 19) % 2;
		const footer = "+" + "-".repeat(msgLen + 2) + "+";
		const header = "+" + "-".repeat(headerLen) + " SIGN SERVER MESSAGE " + "-".repeat(headerLen + paddingLen) + "+";
		return `\n\t${"|" + " ".repeat(header.length - 2) + "|"}\n\t${header}\n\t${"| " + message + " |"}\n\t${footer}\n`;
	}
};
var SignatureRateLimitError = class SignatureRateLimitError extends SignAPIError {
	retryAfter;
	resetTime;
	constructor(apiMessage, formatStr, response) {
		const retryAfter = SignatureRateLimitError.calculateRetryAfter(response);
		const resetTime = SignatureRateLimitError.calculateResetTime(response);
		const logId = response.headers["x-log-id"];
		const agentId = response.headers["x-agent-id"];
		const args = [formatStr.replace("%s", retryAfter.toString())];
		if (apiMessage) {
			const serverMsg = SignAPIError.formatSignServerMessage(apiMessage);
			args.push(serverMsg);
		}
		super("Rate Limited", logId, agentId, ...args);
		this.retryAfter = retryAfter;
		this.resetTime = resetTime;
	}
	static parseHeaderNumber(value) {
		return value ? parseInt(value) : void 0;
	}
	static calculateRetryAfter(response) {
		return parseInt(response.headers["retry-after"] || "0") * 1e3;
	}
	static calculateResetTime(response) {
		const value = response.headers["x-ratelimit-reset"];
		return value ? parseInt(value) * 1e3 : void 0;
	}
};
var SignatureMissingTokensError = class extends SignAPIError {
	constructor(...args) {
		super("Empty Payload", void 0, void 0, ...args);
	}
};
var PremiumFeatureError = class extends SignAPIError {
	constructor(apiMessage, ...args) {
		args.push(SignAPIError.formatSignServerMessage(apiMessage));
		super("Premium Feature", void 0, void 0, ...args);
	}
};
var AuthenticatedWebSocketConnectionError = class extends SignAPIError {
	constructor(...args) {
		super("Authenticated WS", void 0, void 0, ...args);
	}
};
//#endregion
//#region src/lib/ws/lib/proto-utils.ts
const unzip = util.promisify(zlib.unzip);
function hasProtoName(protoName) {
	return !!tikTokSchema[protoName];
}
const WebcastDeserializeConfig = {
	skipMessageTypes: [],
	showBase64OnDecodeError: true
};
function deserializeMessage(protoName, binaryMessage) {
	const messageFn = tikTokSchema[protoName];
	if (!messageFn) throw new InvalidSchemaNameError(`Invalid schema name: ${protoName}`);
	let deserializedMessage;
	try {
		deserializedMessage = messageFn.decode(binaryMessage);
	} catch (ex) {
		let errStr = `Failed to decode message type: ${protoName}: ` + ex.stack;
		if (WebcastDeserializeConfig.showBase64OnDecodeError) errStr += "Base64 - Use this to create an issue: " + base64Encode(new Uint8Array(binaryMessage));
		throw new SchemaDecodeError(errStr);
	}
	if (protoName === "ProtoMessageFetchResult") for (const message of deserializedMessage.messages || []) {
		if (WebcastDeserializeConfig.skipMessageTypes.includes(message.type)) continue;
		if (!hasProtoName(message.type)) {
			if (process.env.DEBUG_DESERIALIZE_XD) {
				console.log("---------------");
				console.log(message.type, base64Encode(Buffer.from(message.payload)));
				console.log("---------------");
			}
			continue;
		}
		try {
			message.decodedData = {
				type: message.type,
				data: deserializeMessage(message.type, Buffer.from(message.payload))
			};
		} catch (ex) {
			message.decodeError = ex;
		}
	}
	return deserializedMessage;
}
async function deserializeWebSocketMessage(binaryMessage) {
	const rawWebcastWebSocketMessage = WebcastPushFrame.decode(binaryMessage);
	let protoMessageFetchResult = void 0;
	if (rawWebcastWebSocketMessage.payloadEncoding === "pb" && rawWebcastWebSocketMessage.payload) {
		let binary = rawWebcastWebSocketMessage.payload;
		if (binary && binary.length > 2 && binary[0] === 31 && binary[1] === 139 && binary[2] === 8) rawWebcastWebSocketMessage.payload = await unzip(binary);
		protoMessageFetchResult = deserializeMessage("ProtoMessageFetchResult", Buffer.from(rawWebcastWebSocketMessage.payload));
	}
	const decodedContainer = rawWebcastWebSocketMessage;
	decodedContainer.protoMessageFetchResult = protoMessageFetchResult;
	return decodedContainer;
}
/**
* Validates and normalizes the uniqueId (username) input for TikTok live room information retrieval.
* It ensures that the input is a string and extracts the username from various possible formats, such as full URLs or with/without '@' symbol. If the input is invalid, it throws an error with a descriptive message.
*
* @param uniqueId Input value representing the unique identifier (username) of a TikTok user. This can be in various formats, such as a plain username, a full TikTok URL, or with an '@' symbol.
*/
function validateAndNormalizeUniqueId(uniqueId) {
	if (typeof uniqueId !== "string") throw new InvalidUniqueIdError("Missing or invalid value for 'uniqueId'. Please provide the username from TikTok URL.");
	return uniqueId.replace("https://www.tiktok.com/", "").replace("/live", "").replace("@", "").trim();
}
/**
* Create a base WebcastPushFrame with default values, allowing overrides for specific fields. This is useful for testing or constructing messages without needing to specify every field.
*
* @param overrides Overrides to apply to the default WebcastPushFrame. Fields not included in the overrides will be set to default values that typically indicate "not set" (e.g., "0" for numeric fields, empty buffer for payload).
*/
function createBaseWebcastPushFrame(overrides) {
	const undefinedNum = "0";
	overrides = Object.fromEntries(Object.entries(overrides).filter(([_, value]) => value !== void 0));
	return WebcastPushFrame.encode({
		seqId: undefinedNum,
		logId: undefinedNum,
		payloadEncoding: "pb",
		payloadType: "msg",
		payload: Buffer.from([]),
		service: undefinedNum,
		method: undefinedNum,
		headers: {},
		...overrides
	});
}
//#endregion
//#region src/lib/web/defaults.ts
/**
* Hint the order of the map by putting a templated value for an item that must be dynamically generated.
*/
const TEMPLATED_VALUE$1 = "[UNRESOLVED]";
const WebcastWebConfigDefaults = {
	TIKTOK_HOST_WEB: "www.tiktok.com",
	TIKTOK_HOST_WEBCAST: "webcast.tiktok.com",
	TIKTOK_HTTP_ORIGIN: "https://www.tiktok.com",
	DEFAULT_HTTP_CLIENT_OPTIONS: {},
	DEFAULT_HTTP_CLIENT_HEADERS: {
		"Cookie": "tt-target-idc=useast1a",
		"Connection": "keep-alive",
		"Cache-Control": "max-age=0",
		"User-Agent": TEMPLATED_VALUE$1,
		"Accept": "text/html,application/json,application/protobuf",
		"Referer": "https://www.tiktok.com/",
		"Origin": "https://www.tiktok.com",
		"Accept-Language": "en-US,en;q=0.9",
		"Accept-Encoding": "gzip, deflate",
		"Sec-Fetch-Site": "same-site",
		"Sec-Fetch-Mode": "cors",
		"Sec-Fetch-Dest": "empty",
		"Sec-Fetch-Ua-Mobile": "?0"
	},
	DEFAULT_HTTP_CLIENT_PARAMS: {
		"aid": 1988 .toString(),
		"app_language": TEMPLATED_VALUE$1,
		"app_name": "tiktok_web",
		"browser_language": TEMPLATED_VALUE$1,
		"browser_name": TEMPLATED_VALUE$1,
		"browser_online": "true",
		"browser_platform": TEMPLATED_VALUE$1,
		"browser_version": TEMPLATED_VALUE$1,
		"cookie_enabled": "true",
		"device_platform": "web_pc",
		"focus_state": "true",
		"from_page": "user",
		"history_len": "10",
		"is_fullscreen": "false",
		"is_page_visible": "true",
		"screen_height": TEMPLATED_VALUE$1,
		"screen_width": TEMPLATED_VALUE$1,
		"tz_name": TEMPLATED_VALUE$1,
		"referer": "https://www.tiktok.com/",
		"root_referer": "https://www.tiktok.com/",
		"channel": "tiktok_web",
		"data_collection_enabled": "true",
		"os": TEMPLATED_VALUE$1,
		"priority_region": TEMPLATED_VALUE$1,
		"region": TEMPLATED_VALUE$1,
		"user_is_login": "true",
		"webcast_language": TEMPLATED_VALUE$1,
		"device_id": TEMPLATED_VALUE$1
	},
	SESSION_COOKIE_NAMES: [
		"sessionid",
		"sessionid_ss",
		"sid_tt",
		"sid_guard"
	],
	TARGET_IDC_COOKIE_NAME: "tt-target-idc"
};
//#endregion
//#region src/lib/ws/defaults.ts
const TEMPLATED_VALUE = "[UNRESOLVED]";
const WebSocketConfigDefaults = {
	TIKTOK_HOST_WS: WebcastWebConfigDefaults.TIKTOK_HOST_WEB,
	DEFAULT_WS_CLIENT_PARAMS: {
		"version_code": "180800",
		"aid": 1988 .toString(),
		"app_language": TEMPLATED_VALUE,
		"app_name": "tiktok_web",
		"browser_platform": TEMPLATED_VALUE,
		"browser_language": TEMPLATED_VALUE,
		"browser_name": TEMPLATED_VALUE,
		"browser_version": TEMPLATED_VALUE,
		"browser_online": "true",
		"cookie_enabled": "true",
		"tz_name": TEMPLATED_VALUE,
		"device_platform": "web",
		"identity": "audience",
		"live_id": "12",
		"webcast_language": TEMPLATED_VALUE,
		"ws_direct": "0",
		"sup_ws_ds_opt": "1",
		"update_version_code": "2.0.0",
		"did_rule": "3",
		"screen_height": TEMPLATED_VALUE,
		"screen_width": TEMPLATED_VALUE,
		"heartbeat_duration": "0",
		"resp_content_type": "protobuf",
		"history_comment_count": "6",
		"client_enter": "1",
		"last_rtt": (Math.random() * 100 + 100).toString()
	},
	DEFAULT_WS_CLIENT_PARAMS_APPEND_PARAMETER: "&version_code=270000",
	DEFAULT_WS_CLIENT_HEADERS: { "User-Agent": TEMPLATED_VALUE },
	DEFAULT_WS_PING_INTERVAL: 1e4
};
//#endregion
//#region src/lib/ws/config.ts
function getWebSocketConfigDefaults({ device, screen, location }) {
	const baseConfig = structuredClone(WebSocketConfigDefaults);
	baseConfig.DEFAULT_WS_CLIENT_PARAMS.app_language = location.lang;
	baseConfig.DEFAULT_WS_CLIENT_PARAMS.browser_platform = device.browser_platform;
	baseConfig.DEFAULT_WS_CLIENT_PARAMS.browser_language = location.lang_country;
	baseConfig.DEFAULT_WS_CLIENT_PARAMS.browser_name = device.browser_name;
	baseConfig.DEFAULT_WS_CLIENT_PARAMS.browser_version = device.browser_version;
	baseConfig.DEFAULT_WS_CLIENT_PARAMS.tz_name = location.tz_name;
	baseConfig.DEFAULT_WS_CLIENT_PARAMS.webcast_language = location.lang;
	baseConfig.DEFAULT_WS_CLIENT_PARAMS.screen_height = screen.screen_height.toString();
	baseConfig.DEFAULT_WS_CLIENT_PARAMS.screen_width = screen.screen_width.toString();
	baseConfig.DEFAULT_WS_CLIENT_PARAMS["User-Agent"] = device.user_agent;
	baseConfig.DEFAULT_WS_CLIENT_HEADERS["User-Agent"] = device.user_agent;
	return baseConfig;
}
//#endregion
//#region src/lib/ws/lib/ws-utils.ts
/**
* Generates a random java.Long for the uniqId string.
*/
function generateUniqId() {
	const bytes = crypto.getRandomValues(new Uint8Array(8));
	bytes[0] &= 127;
	let val = 0n;
	for (const b of bytes) val = val << 8n | BigInt(b);
	if (val === 0n) val = 1n;
	return val.toString();
}
//#endregion
//#region src/lib/ws/lib/ws-client.ts
const textEncoder = new TextEncoder();
/**
* Creates a WebSocket provider function that can be used to create WebcastWebSocketClient instances with dynamic parameters.
*
* @param webcastWebSocketConfig The default WebSocket configuration to use for all clients created by this provider. Dynamic parameters will override these defaults.
* @param wsClientOptions The WebSocket client options to use for all clients created by this provider. These options will be merged with any dynamic parameters provided when creating a client.
*
*/
function createWebSocketProvider(webcastWebSocketConfig, wsClientOptions) {
	return (dynamicParams) => {
		return new WebcastWebSocketClient({
			...webcastWebSocketConfig,
			...dynamicParams
		}, wsClientOptions);
	};
}
var WebcastWebSocketClient = class extends WebSocket {
	pingInterval;
	seqId = 1;
	enterUniqueId = null;
	roomId;
	webcastWsConfig;
	constructor(webcastWebSocketConfig, wsClientOptions) {
		const { headers: wsHeaders, ...reducedWsClientOptions } = wsClientOptions || {};
		const cleanWebcastConfig = structuredClone(webcastWebSocketConfig);
		cleanWebcastConfig.DEFAULT_WS_CLIENT_PARAMS = {
			...cleanWebcastConfig.DEFAULT_WS_CLIENT_PARAMS,
			...webcastWebSocketConfig.wsParams
		};
		cleanWebcastConfig.DEFAULT_WS_CLIENT_HEADERS = {
			...cleanWebcastConfig.DEFAULT_WS_CLIENT_HEADERS,
			...wsHeaders,
			...webcastWebSocketConfig.wsHeaders
		};
		const webSocketUrl = webcastWebSocketConfig.baseUrl + `?${new URLSearchParams(cleanWebcastConfig.DEFAULT_WS_CLIENT_PARAMS)}${cleanWebcastConfig.DEFAULT_WS_CLIENT_PARAMS_APPEND_PARAMETER}`;
		super(webSocketUrl, {
			headers: cleanWebcastConfig.DEFAULT_WS_CLIENT_HEADERS,
			host: cleanWebcastConfig.TIKTOK_HOST_WS,
			...reducedWsClientOptions,
			autoPong: false
		});
		this.webcastWsConfig = cleanWebcastConfig;
		this.pingInterval = null;
		this.on("message", this.onMessage.bind(this));
		this.on("close", this.onDisconnect.bind(this));
	}
	get open() {
		return this.readyState === WebSocket.OPEN;
	}
	/**
	* Send a message to the WebSocket server
	* @param data The message to send
	* @returns True if the message was sent, false otherwise
	*/
	sendBytes(data) {
		if (this.open) {
			super.send(Buffer.from(data));
			return true;
		}
		return false;
	}
	onDisconnect() {
		if (this.pingInterval) clearInterval(this.pingInterval);
		this.pingInterval = null;
		this.seqId = 1;
	}
	/**
	* Handle incoming messages
	* @param message The incoming WebSocket message (type => Buffer)
	* @protected
	*/
	async onMessage(message) {
		this.emit("webSocketData", message);
		try {
			const decodedContainer = await deserializeWebSocketMessage(message);
			if (decodedContainer.protoMessageFetchResult) {
				if (decodedContainer.protoMessageFetchResult.needsAck) this.sendAck(decodedContainer);
				this.emit("protoMessageFetchResult", decodedContainer.protoMessageFetchResult);
			}
			if (decodedContainer.payloadType === "im_enter_room_resp") this.emit("imEnteredRoom", decodedContainer);
		} catch (err) {
			this.emit("messageDecodingFailed", err);
		}
	}
	/**
	* Static Keep-Alive ping
	*/
	sendHeartbeat() {
		const hb = HeartbeatMessage.encode({
			roomId: this.webcastWsConfig.roomId,
			sendPacketSeqId: String(this.seqId)
		});
		const webcastPushFrame = createBaseWebcastPushFrame({
			payloadEncoding: "pb",
			payloadType: "hb",
			payload: Buffer.from(hb.finish()),
			service: void 0,
			method: void 0,
			headers: {}
		});
		this.sendBytes(Buffer.from(webcastPushFrame.finish()));
		this.seqId++;
	}
	/**
	* Switch to a different TikTok LIVE room while connected to the WebSocket
	*
	* @param roomId The room ID to switch to
	*/
	switchRooms(roomId) {
		this.seqId = 1;
		this.enterUniqueId = generateUniqId();
		const imEnterRoomMessage = WebcastImEnterRoomMessage.encode({
			roomId,
			roomTag: "",
			liveRegion: "",
			liveId: "12",
			identity: "audience",
			cursor: "",
			accountType: "0",
			enterUniqueId: this.enterUniqueId,
			filterWelcomeMsg: "0",
			isAnchorContinueKeepMsg: false
		});
		const webcastPushFrame = createBaseWebcastPushFrame({
			payloadEncoding: "pb",
			payloadType: "im_enter_room",
			payload: Buffer.from(imEnterRoomMessage.finish())
		});
		this.sendBytes(Buffer.from(webcastPushFrame.finish()));
		if (this.pingInterval) clearInterval(this.pingInterval);
		this.pingInterval = setInterval(() => this.sendHeartbeat(), this.webcastWsConfig.DEFAULT_WS_PING_INTERVAL);
	}
	/**
	* Acknowledge the message was received
	*/
	sendAck({ logId, protoMessageFetchResult }) {
		if (!logId) return;
		if (!protoMessageFetchResult?.internalExt) {
			if (!process.env.DISABLE_ACK_LOG_WARNING) console.error("No internalExt found for message that needs ACK, skipping ACK");
			return;
		}
		const webcastPushFrame = createBaseWebcastPushFrame({
			logId,
			payloadEncoding: "pb",
			payloadType: "ack",
			payload: Buffer.from(textEncoder.encode(protoMessageFetchResult.internalExt))
		});
		this.sendBytes(Buffer.from(webcastPushFrame.finish()));
	}
};
//#endregion
//#region src/types/events.ts
let ControlEvent = /* @__PURE__ */ function(ControlEvent) {
	ControlEvent["CONNECTED"] = "connected";
	ControlEvent["DISCONNECTED"] = "disconnected";
	ControlEvent["ERROR"] = "error";
	ControlEvent["RAW_DATA"] = "rawData";
	ControlEvent["DECODED_DATA"] = "decodedData";
	ControlEvent["WEBSOCKET_CONNECTED"] = "websocketConnected";
	ControlEvent["WEBSOCKET_DATA"] = "websocketData";
	ControlEvent["ENTER_ROOM"] = "enterRoom";
	return ControlEvent;
}({});
let WebcastEvent = /* @__PURE__ */ function(WebcastEvent) {
	WebcastEvent["CHAT"] = "chat";
	WebcastEvent["MEMBER"] = "member";
	WebcastEvent["GIFT"] = "gift";
	WebcastEvent["ROOM_USER"] = "roomUser";
	WebcastEvent["SOCIAL"] = "social";
	WebcastEvent["LIKE"] = "like";
	WebcastEvent["QUESTION_NEW"] = "questionNew";
	WebcastEvent["LINK_MIC_BATTLE"] = "linkMicBattle";
	WebcastEvent["LINK_MIC_ARMIES"] = "linkMicArmies";
	WebcastEvent["LIVE_INTRO"] = "liveIntro";
	WebcastEvent["EMOTE"] = "emote";
	WebcastEvent["ENVELOPE"] = "envelope";
	WebcastEvent["FOLLOW"] = "follow";
	WebcastEvent["SHARE"] = "share";
	WebcastEvent["STREAM_END"] = "streamEnd";
	WebcastEvent["CONTROL_MESSAGE"] = "controlMessage";
	WebcastEvent["BARRAGE"] = "barrage";
	WebcastEvent["SYSTEM"] = "system";
	WebcastEvent["HOURLY_RANK"] = "hourlyRank";
	WebcastEvent["GOAL_UPDATE"] = "goalUpdate";
	WebcastEvent["ROOM_MESSAGE"] = "roomMessage";
	WebcastEvent["CAPTION_MESSAGE"] = "captionMessage";
	WebcastEvent["IM_DELETE"] = "imDelete";
	WebcastEvent["IN_ROOM_BANNER"] = "inRoomBanner";
	WebcastEvent["RANK_UPDATE"] = "rankUpdate";
	WebcastEvent["POLL_MESSAGE"] = "pollMessage";
	WebcastEvent["RANK_TEXT"] = "rankText";
	WebcastEvent["LINK_MIC_BATTLE_PUNISH_FINISH"] = "linkMicBattlePunishFinish";
	WebcastEvent["LINK_MIC_BATTLE_TASK"] = "linkMicBattleTask";
	WebcastEvent["LINK_MIC_FAN_TICKET_METHOD"] = "linkMicFanTicketMethod";
	WebcastEvent["LINK_MIC_METHOD"] = "linkMicMethod";
	WebcastEvent["UNAUTHORIZED_MEMBER"] = "unauthorizedMember";
	WebcastEvent["OEC_LIVE_SHOPPING"] = "oecLiveShopping";
	WebcastEvent["MSG_DETECT"] = "msgDetect";
	WebcastEvent["LINK_MESSAGE"] = "linkMessage";
	WebcastEvent["ROOM_VERIFY"] = "roomVerify";
	WebcastEvent["LINK_LAYER"] = "linkLayer";
	WebcastEvent["ROOM_PIN"] = "roomPin";
	WebcastEvent["SUPER_FAN"] = "superFan";
	WebcastEvent["SUPER_FAN_JOIN"] = "superFanJoin";
	WebcastEvent["SUPER_FAN_BOX"] = "superFanBox";
	return WebcastEvent;
}({});
let ConnectState = /* @__PURE__ */ function(ConnectState) {
	ConnectState["DISCONNECTED"] = "DISCONNECTED";
	ConnectState["CONNECTING"] = "CONNECTING";
	ConnectState["CONNECTED"] = "CONNECTED";
	return ConnectState;
}({});
const WebcastEventMap = {
	"WebcastChatMessage": "chat",
	"WebcastMemberMessage": "member",
	"WebcastRoomUserSeqMessage": "roomUser",
	"WebcastSocialMessage": "social",
	"WebcastLikeMessage": "like",
	"WebcastQuestionNewMessage": "questionNew",
	"WebcastLinkMicBattle": "linkMicBattle",
	"WebcastLinkMicArmies": "linkMicArmies",
	"WebcastLiveIntroMessage": "liveIntro",
	"WebcastEmoteChatMessage": "emote",
	"WebcastEnvelopeMessage": "envelope",
	"WebcastBarrageMessage": "barrage",
	"WebcastSystemMessage": "system",
	"WebcastHourlyRankMessage": "hourlyRank",
	"WebcastGoalUpdateMessage": "goalUpdate",
	"WebcastRoomMessage": "roomMessage",
	"WebcastCaptionMessage": "captionMessage",
	"WebcastControlMessage": "controlMessage",
	"WebcastImDeleteMessage": "imDelete",
	"WebcastInRoomBannerMessage": "inRoomBanner",
	"WebcastRankUpdateMessage": "rankUpdate",
	"WebcastPollMessage": "pollMessage",
	"WebcastRankTextMessage": "rankText",
	"WebcastLinkMicBattlePunishFinish": "linkMicBattlePunishFinish",
	"WebcastLinkmicBattleTaskMessage": "linkMicBattleTask",
	"WebcastLinkMicFanTicketMethod": "linkMicFanTicketMethod",
	"WebcastLinkMicMethod": "linkMicMethod",
	"WebcastUnauthorizedMemberMessage": "unauthorizedMember",
	"WebcastOecLiveShoppingMessage": "oecLiveShopping",
	"WebcastMsgDetectMessage": "msgDetect",
	"WebcastLinkMessage": "linkMessage",
	"RoomVerifyMessage": "roomVerify",
	"WebcastLinkLayerMessage": "linkLayer",
	"WebcastRoomPinMessage": "roomPin"
};
//#endregion
//#region src/lib/web/lib/cookie-jar.ts
/**
* Custom cookie jar for got
*
* Exposes `beforeRequest` / `afterResponse` hooks that the HTTP client wires into `got.extend(...)`,
* mirroring the axios interceptor model from before the migration.
*
* https://github.com/zerodytrash/TikTok-Livestream-Chat-Connector/issues/18
*/
var WebcastCookieJar = class WebcastCookieJar {
	/**
	* The internal cookie store, a simple key-value object. The keys are cookie names, the values are cookie values.
	*/
	store;
	/**
	* Constructor
	*
	* @param webConfig The current web config for the HTTP client
	*/
	constructor(webConfig) {
		this.webConfig = webConfig;
		this.store = WebcastCookieJar.parseCookies(webConfig.DEFAULT_HTTP_CLIENT_HEADERS.Cookie);
	}
	/**
	* Set the session ID and tt-target-idc
	*
	* @param session The session bundle containing the session ID and tt-target-idc
	*/
	async setSessionBundle(session) {
		if (session.type !== "cookie") throw new TypeError("Invalid session bundle type. Expected 'cookie'.");
		this.webConfig.SESSION_COOKIE_NAMES.forEach((cookie) => this.store[cookie] = session.value.sessionId);
		this.store[this.webConfig.TARGET_IDC_COOKIE_NAME] = session.value.ttTargetIdc;
	}
	/**
	* Get the session bundle containing the session ID and tt-target-idc
	*
	*/
	async getSessionBundle() {
		let ttTargetIdc = this.store[this.webConfig.TARGET_IDC_COOKIE_NAME] || null;
		let sessionId = null;
		for (const cookieName of this.webConfig.SESSION_COOKIE_NAMES) if (this.store[cookieName]) {
			sessionId = this.store[cookieName];
			break;
		}
		if (ttTargetIdc && sessionId) return {
			type: "cookie",
			value: {
				ttTargetIdc,
				sessionId
			}
		};
		return null;
	}
	/**
	* Process a single set-cookie header
	*
	* @param setCookieHeader The set-cookie header
	*/
	async processSetCookieHeader(setCookieHeader) {
		if (typeof setCookieHeader === "string") {
			const parts = setCookieHeader.split(";")[0].split("=");
			const cookieName = parts.shift();
			const cookieValue = parts.join("=");
			if (typeof cookieName === "string" && cookieName !== "") this.store[decodeURIComponent(cookieName)] = decodeURIComponent(cookieValue);
		}
	}
	/**
	* We ignore the URL parameter because TikTok's cookies are not scoped by path or domain - any cookie we set should be sent with every request to TikTok regardless of URL
	*
	* @param __url Ignored
	*/
	async getCookieString(__url = "") {
		return WebcastCookieJar.serializeCookieObject(this.store);
	}
	/**
	* Set cookie string - we ignore the URL parameter for the same reason as in getCookieString
	* @param rawCookie The raw cookie string from the set-cookie header
	* @param __url The URL the cookie is associated with (ignored)
	*/
	async setCookie(rawCookie, __url = "") {
		if (!rawCookie) return;
		await this.processSetCookieHeader(rawCookie);
	}
	/**
	* Serialize a cookie object into a cookie header string. For example, { sessionid: 'abc', tt-target-idc: 'def' } becomes 'sessionid=abc; tt-target-idc=def'
	*
	* @param cookies The cookie object to serialize
	*/
	static serializeCookieObject(cookies) {
		return Object.entries(cookies).map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`).join("; ");
	}
	/**
	* Serialize a CookieSessionBundle into a Record of cookie name to cookie value, based on the provided web config. This is used to set the initial cookies in the cookie jar when creating a new session.
	*
	* @param config The web config containing the cookie names to use for the session ID and tt-target-idc
	* @param session The session bundle containing the session ID and tt-target-idc to serialize
	*
	*/
	static serializeCookieSessionBundle(config, session) {
		const cookies = {};
		config.SESSION_COOKIE_NAMES.forEach((cookieName) => {
			cookies[cookieName] = session.value.sessionId;
		});
		cookies[config.TARGET_IDC_COOKIE_NAME] = session.value.ttTargetIdc;
		return WebcastCookieJar.serializeCookieObject(cookies);
	}
	/**
	* Parse a cookie header string into a Record of cookie name to cookie value. For example, 'sessionid=abc; tt-target-idc=def' becomes { sessionid: 'abc', tt-target-idc: 'def' }
	*
	* @param str
	*/
	static parseCookies(str) {
		const cookies = {};
		if (!str) return cookies;
		str.split("; ").forEach((v) => {
			if (!v) return;
			const parts = String(v).split("=");
			const cookieName = decodeURIComponent(parts.shift() || "");
			cookies[cookieName] = parts.join("=");
		});
		return cookies;
	}
};
//#endregion
//#region src/version.ts
const VERSION = "2.3.0-alpha1";
//#endregion
//#region src/lib/web/routes/euler/config.ts
/**
* Should never be changed, used to identify the library in requests to TikTok's APIs
*/
const LIBRARY_IDENTITY = "ttlive-node";
const SignConfig = {
	basePath: process.env.SIGN_API_URL || "https://tiktok.eulerstream.com",
	apiKey: process.env.SIGN_API_KEY,
	baseOptions: {
		headers: { "User-Agent": `tiktok-live-connector/${VERSION} ${process.platform}` },
		validateStatus: () => true
	},
	cachedInstance: void 0
};
/**
* Creates (or retrieves from cache) an instance of the Euler Stream API client configured with SignConfig.
*
* Should be a global singleton to avoid unnecessary re-instantiations and to allow for dynamic config changes (e.g. API key rotation).
*
*/
function createEulerClient() {
	const { cachedInstance, ...clientConfig } = { ...SignConfig };
	if (cachedInstance) return cachedInstance;
	SignConfig.cachedInstance = new EulerStreamApiClient(clientConfig);
	return SignConfig.cachedInstance;
}
//#endregion
//#region src/lib/web/lib/route-wrapper.ts
function createRoute(routeId, routeHandler) {
	return (args) => routeHandler({
		...args,
		routeId
	});
}
//#endregion
//#region src/lib/web/routes/routes.ts
let BaseFetchRoute = /* @__PURE__ */ function(BaseFetchRoute) {
	BaseFetchRoute["FETCH_ROOM_GIFTS"] = "fetchRoomGiftsRoute";
	BaseFetchRoute["FETCH_ROOM_INFO"] = "fetchRoomInfoRoute";
	BaseFetchRoute["FETCH_ROOM_INFO_API_LIVE"] = "fetchRoomInfoApiLiveRoute";
	BaseFetchRoute["FETCH_ROOM_INFO_HTML"] = "fetchRoomInfoHtmlRoute";
	return BaseFetchRoute;
}({});
let CompositeFetchRoute = /* @__PURE__ */ function(CompositeFetchRoute) {
	CompositeFetchRoute["FETCH_IS_LIVE"] = "fetchIsLiveRoute";
	CompositeFetchRoute["FETCH_ROOM_ID"] = "fetchRoomIdRoute";
	return CompositeFetchRoute;
}({});
let EulerFetchRoute = /* @__PURE__ */ function(EulerFetchRoute) {
	EulerFetchRoute["FETCH_ROOM_ID"] = "fetchRoomIdFromEulerRoute";
	EulerFetchRoute["FETCH_ROOM_INFO"] = "fetchRoomInfoFromEulerRoute";
	EulerFetchRoute["FETCH_SIGNED_WEBSOCKET"] = "fetchSignedWebSocketFromEulerRoute";
	EulerFetchRoute["FETCH_WEBCAST_SIGNATURE"] = "fetchWebcastSignatureFromEulerRoute";
	EulerFetchRoute["SEND_ROOM_CHAT"] = "sendRoomChatFromEulerRoute";
	return EulerFetchRoute;
}({});
//#endregion
//#region src/lib/web/routes/euler/fetch-room-id-euler.ts
/**
* Fetches a room ID from Euler Stream using the provided uniqueId.
*
* @param apiClient The Euler Stream API client to use for the request.
* @param uniqueId  The unique identifier (username) of the TikTok user whose room ID is being fetched.
*/
const fetchRoomIdFromEulerRoute = createRoute("fetchRoomIdFromEulerRoute", async ({ apiClient, uniqueId, options }) => {
	return (await apiClient.webcast.retrieveRoomId(uniqueId, options)).data;
});
//#endregion
//#region src/lib/web/routes/euler/fetch-room-info-euler.ts
/**
* Fetches full room information (streamer info, stats, status) from Euler Stream's premium endpoint.
*
* @param apiClient The Euler Stream API client to use for the request.
* @param uniqueId  The unique identifier (username) of the TikTok user whose room info is being fetched.
*/
const fetchRoomInfoFromEulerRoute = createRoute("fetchRoomInfoFromEulerRoute", async ({ apiClient, uniqueId, options }) => {
	return (await apiClient.premium.retrieveRoomInfo(uniqueId, options)).data;
});
//#endregion
//#region src/lib/web/routes/euler/fetch-signed-websocket-euler.ts
/**
* Fetches a signed TikTok WebSocket URL from Euler Stream along with the initial `ProtoMessageFetchResult`
* and any cookies the sign server set.
*
* Enforces whitelist validation (via `WHITELIST_AUTHENTICATED_SESSION_ID_HOST`) when authenticated WebSocket
* connections are requested. Translates sign-server HTTP responses into typed errors
* (`SignAPIError`, `SignatureRateLimitError`, `PremiumFeatureError`).
*/
const fetchSignedWebSocketFromEulerRoute = createRoute("fetchSignedWebSocketFromEulerRoute", async ({ routeId, roomId, apiClient, webClient, useMobile, session, authenticateWs, country, cursor, options }) => {
	if (session && authenticateWs) {
		const envHost = process.env.WHITELIST_AUTHENTICATED_SESSION_ID_HOST;
		const expectedHost = apiClient.configuration.basePath ? new URL(apiClient.configuration.basePath).host : void 0;
		if (!envHost) throw new AuthenticatedWebSocketConnectionError(`authenticate_websocket is true, but no whitelist host defined. Set the env var WHITELIST_AUTHENTICATED_SESSION_ID_HOST to proceed.`);
		if (envHost !== expectedHost) throw new AuthenticatedWebSocketConnectionError(`The env var WHITELIST_AUTHENTICATED_SESSION_ID_HOST "${envHost}" does not match sign server host "${expectedHost}".`);
	}
	const xCookieHeader = await webClient.cookieJar.getCookieString() || void 0;
	const xOauthToken = xCookieHeader ? void 0 : webClient.oAuthSessionBundle?.value || void 0;
	if (authenticateWs && !xCookieHeader && !xOauthToken) throw new AuthenticatedWebSocketConnectionError(`authenticate_websocket is true, but no session cookies or OAuth token found.`);
	let response;
	try {
		response = await apiClient.webcast.fetchWebcastURL(LIBRARY_IDENTITY, roomId, void 0, cursor, webClient.clientHeaders["User-Agent"], true, country, useMobile ? WebcastFetchPlatform.Mobile : WebcastFetchPlatform.Web, authenticateWs ? xOauthToken : void 0, authenticateWs ? xCookieHeader : void 0, void 0, void 0, {
			...options,
			responseType: "arraybuffer"
		});
	} catch (err) {
		throw new SignAPIError("Connect Error", void 0, void 0, "Failed to connect to sign server.", err);
	}
	if (response.status === 429) {
		const data = JSON.parse(Buffer.from(response.data).toString("utf-8"));
		throw new SignatureRateLimitError(process.env.SIGN_SERVER_MESSAGE_DISABLED ? null : data?.message, `${data?.limit_label ? `(${data.limit_label}) ` : ""}Too many connections started, try again later.`, response);
	}
	if (response.status === 402) {
		const data = JSON.parse(Buffer.from(response.data).toString("utf-8"));
		throw new PremiumFeatureError(process.env.SIGN_SERVER_MESSAGE_DISABLED ? null : data?.message, "Error fetching the signed TikTok WebSocket");
	}
	const logId = response.headers["x-request-id"];
	const agentId = response.headers["x-agent-id"];
	if (response.status !== 200) {
		let payload;
		try {
			payload = Buffer.from(response.data).toString("utf-8");
		} catch {
			payload = `"${response.statusText}"`;
		}
		throw new SignAPIError("Sign Error", logId, agentId, `[${routeId}] Unexpected sign server status ${response.status}. Payload:\n${payload}`);
	}
	if (!response.headers["x-set-tt-cookie"]) throw new SignAPIError("Empty Cookies", logId, agentId, `[${routeId}] No cookies received from sign server.`);
	return {
		fetchResult: deserializeMessage("ProtoMessageFetchResult", Buffer.from(response.data)),
		fetchResultCookieHeader: response.headers["x-set-tt-cookie"],
		fetchResultRoomId: response.headers["x-room-id"]
	};
});
//#endregion
//#region src/lib/web/routes/euler/fetch-webcast-signature-euler.ts
/**
* Signs an arbitrary TikTok webcast URL through the Euler Stream sign server, returning the
* signed URL and token bundle the caller needs to make the request.
*
* Strips known signature parameters (X-Bogus, X-Gnarly, msToken) from the input URL before signing.
* If `webClient.cookieJar` already holds a `sessionid` and `tt-target-idc` pair, both are forwarded
* to the sign server so the resulting signature is bound to that session.
*
* @param apiClient The Euler Stream API client used to issue the sign request.
* @param webClient The HTTP client whose cookie jar provides any optional session bundle.
* @param url The original TikTok URL (string or `URL`) to sign.
* @param method HTTP method the signed URL will be used with.
* @param userAgent User-Agent string the signed request will use.
* @param options Optional axios request overrides forwarded to the SDK call.
*/
const fetchWebcastSignatureFromEulerRoute = createRoute("fetchWebcastSignatureFromEulerRoute", async ({ routeId, url, method, userAgent, apiClient, webClient, options }) => {
	const session = await webClient.cookieJar.getSessionBundle();
	const sessionId = session?.value.sessionId;
	const ttTargetIdc = session?.value.ttTargetIdc;
	const mustRemoveParams = [
		"X-Bogus",
		"X-Gnarly",
		"msToken"
	];
	let cleanUrl = typeof url === "string" ? url : url.toString();
	for (const param of mustRemoveParams) {
		cleanUrl = cleanUrl.replace(new RegExp(`([&?])${param}=[^&]*`, "g"), "$1");
		cleanUrl = cleanUrl.replace(/[&?]$/, "");
	}
	if (sessionId && !ttTargetIdc) throw new InvalidRequestError({ routeId }, "ttTargetIdc must be set when sessionId is provided.");
	const response = await apiClient.webcast.signWebcastUrl({
		url: cleanUrl,
		method,
		userAgent,
		sessionId,
		ttTargetIdc
	}, LIBRARY_IDENTITY, options);
	if (response.status === 403) throw new PremiumFeatureError("You do not have permission from the signature provider to sign this URL.", response.data.message || "Forbidden", JSON.stringify(response.data));
	if (response.status !== 200) throw new SignatureMissingTokensError(`[${routeId}] Failed to sign a request: ${response?.data?.message || "Unknown error"}`);
	if (!response.data || Object.keys(response.data?.response?.tokens || {}).length < 1) throw new SignatureMissingTokensError(`[${routeId}] Failed to sign a request due to missing tokens in response!`);
	return response.data;
});
//#endregion
//#region src/lib/web/routes/euler/send-room-chat-euler.ts
/**
* Sends a chat message into a TikTok LIVE room via Euler Stream's premium endpoint. Supports either
* cookie-based (`sessionId` + `ttTargetIdc`) or OAuth-token session bundles; OAuth takes precedence when present.
*
* Throws `PremiumFeatureError` when the sign server returns 401/403, and `InvalidResponseError` for
* any other non-200 response.
*/
const sendRoomChatFromEulerRoute = createRoute("sendRoomChatFromEulerRoute", async ({ routeId, webClient, apiClient, roomId, content, options }) => {
	roomId ||= webClient.roomId;
	if (!roomId) throw new InvalidRequestError({ routeId }, "Room ID must be specified in all cases to send a chat to a room");
	const xCookieHeader = await webClient.cookieJar.getCookieString() || void 0;
	const xOauthToken = xCookieHeader ? void 0 : webClient.oAuthSessionBundle?.value || void 0;
	const fetchResponse = await apiClient.premium.sendRoomChat({
		content,
		targetRoomId: roomId,
		sessionId: "",
		ttTargetIdc: ""
	}, xOauthToken, xCookieHeader, options);
	switch (fetchResponse.status) {
		case 401:
		case 403: throw new PremiumFeatureError("Sending chats requires an API key & a paid plan, as it uses cloud managed services.", fetchResponse.data.message || "Unauthorized", JSON.stringify(fetchResponse.data));
		case 200: return fetchResponse.data;
		default: throw new InvalidResponseError({ routeId }, `Failed to send chat: ${fetchResponse?.data?.message || "Unknown error"}`);
	}
});
//#endregion
//#region src/lib/web/routes/base/fetch-room-gifts.ts
/**
* Fetches the list of gifts available in a TikTok LIVE room. If `roomId` is not provided, the
* `roomId` currently set on the `webClient` is used.
*
* @param webClient The HTTP client instance to use for the request.
* @param roomId The ID of the room whose gift list to fetch. Optional if the webClient has a roomId context.
*/
const fetchRoomGiftsRoute = createRoute("fetchRoomGiftsRoute", async ({ routeId, webClient, roomId = webClient.roomId }) => {
	if (!roomId) throw new InvalidRequestError({ routeId }, "Missing roomId. Please provide a roomId to the HTTP client.");
	return (await webClient.getJsonObjectFromWebcastApi("gift/list/", {
		...webClient.clientParams,
		room_id: roomId
	}, false)).data.gifts;
});
//#endregion
//#region src/lib/web/routes/base/fetch-room-info.ts
/**
* Fetches room information for a given roomId. If roomId is not provided, it will attempt to use the roomId from the webClient context.
*
* @param webClient The HTTP client instance to use for the request.
* @param roomId The ID of the room to fetch information for. Optional if the webClient has a roomId context.
*/
const fetchRoomInfoRoute = createRoute("fetchRoomInfoRoute", async ({ routeId, webClient, roomId = webClient.roomId }) => {
	if (!roomId) throw new InvalidRequestError({ routeId }, "Missing roomId. Please provide a roomId to the HTTP client.");
	return await webClient.getJsonObjectFromWebcastApi("room/info/", {
		...webClient.clientParams,
		room_id: roomId
	}, false);
});
//#endregion
//#region src/lib/web/routes/base/fetch-room-info-api-live.ts
/**
* Fetches room information from the TikTok API Live endpoint using the provided uniqueId. This route is used as part of the process to determine if a user is currently live streaming and to retrieve associated room information.
*
* @param webClient The HTTP client instance to use for making the API request.
* @param uniqueId The unique identifier (username) of the TikTok user whose room information is being fetched.
*/
const fetchRoomInfoFromApiLiveRoute = createRoute("fetchRoomInfoApiLiveRoute", async ({ webClient, uniqueId, routeId }) => {
	const roomData = await webClient.getJsonObjectFromTikTokApi("api-live/user/room/", {
		...webClient.clientParams,
		uniqueId,
		sourceType: "54"
	});
	if (roomData.statusCode) throw new InvalidResponseError({ routeId }, `API Error ${roomData.statusCode} (${roomData.message || "Unknown Error"})`);
	if (!roomData?.data?.user?.roomId) throw new InvalidResponseError({ routeId }, `Invalid response from API: ${JSON.stringify(roomData)}`);
	return roomData;
});
//#endregion
//#region src/lib/web/routes/base/fetch-room-info-html.ts
/**
* Mutable configuration for `fetchRoomInfoFromHtmlRoute`. Override `extractionPattern`
* if TikTok ever changes the markup wrapping the SIGI_STATE JSON blob.
*/
const RoomInfoFromHtmlRouteConfig = { extractionPattern: /<script id="SIGI_STATE" type="application\/json">(.*?)<\/script>/ };
/**
* Fetches room information by scraping the HTML content of the TikTok user's live page.
* This route is used as a fallback method to retrieve room information when API endpoints are unavailable or blocked. It extracts the SIGI_STATE JSON data embedded in the HTML, which contains various details about the live room and user information.
*
* @param webClient The HTTP client instance to use for making the request to the TikTok website.
* @param uniqueId The unique identifier (username) of the TikTok user whose room information is being fetched.
*/
const fetchRoomInfoFromHtmlRoute = createRoute("fetchRoomInfoHtmlRoute", async ({ webClient, uniqueId, routeId }) => {
	const match = (await webClient.getHtmlFromTikTokWebsite(`@${uniqueId}/live`)).match(RoomInfoFromHtmlRouteConfig.extractionPattern);
	if (!match || match.length < 2) throw new InvalidResponseError({ routeId }, "Failed to extract the SIGI_STATE HTML tag, you might be blocked by TikTok.");
	let sigiState;
	try {
		sigiState = JSON.parse(match[1]);
	} catch (e) {
		throw new InvalidResponseError({ routeId }, "Failed to parse SIGI_STATE into JSON. Are you captcha-blocked by TikTok?");
	}
	const liveRoom = sigiState?.LiveRoom?.liveRoomUserInfo;
	if (!liveRoom) throw new InvalidResponseError({ routeId }, "Failed to extract the LiveRoom object from SIGI_STATE.");
	return liveRoom;
});
//#endregion
//#region src/lib/web/routes/composite/fetch-room-id.ts
const RoomIdRouteConfig = {
	skipFetchRoomInfoFromHtmlRoute: false,
	skipFetchRoomInfoFromApiLiveRoute: false,
	skipFetchRoomIdFromEulerRoute: false
};
/**
* Resolves a room ID across multiple sources, falling back progressively:
*   1. HTML scrape (SIGI_STATE)
*   2. TikTok API Live endpoint
*   3. Euler Stream (skipped if `RoomIdRouteConfig.skipFetchRoomIdFromEulerRoute` is true)
*
* Errors from each source are accumulated and surfaced via `InvalidResponseCompositeError` if every source fails.
*/
const fetchRoomIdComposite = createRoute("fetchRoomIdRoute", async ({ routeId, uniqueId, webClient, apiClient }) => {
	const errors = [];
	if (!RoomIdRouteConfig.skipFetchRoomInfoFromHtmlRoute) try {
		const roomId = (await fetchRoomInfoFromHtmlRoute({
			webClient,
			uniqueId
		})).user.roomId;
		if (!roomId) throw new InvalidResponseError({ routeId }, "Failed to extract Room ID from HTML.");
		return roomId;
	} catch (ex) {
		errors.push(ex);
	}
	if (!RoomIdRouteConfig.skipFetchRoomInfoFromApiLiveRoute) try {
		const roomId = (await fetchRoomInfoFromApiLiveRoute({
			webClient,
			uniqueId
		}))?.data?.user?.roomId;
		if (!roomId) throw new InvalidResponseError({ routeId }, "Failed to extract Room ID from API.");
		return roomId;
	} catch (ex) {
		errors.push(ex);
	}
	if (!RoomIdRouteConfig.skipFetchRoomIdFromEulerRoute) try {
		const response = await fetchRoomIdFromEulerRoute({
			webClient,
			apiClient,
			uniqueId
		});
		if ([
			403,
			402,
			401
		].includes(response.code)) throw new InvalidResponseError({ routeId }, "Failed to retrieve Room ID from Euler Stream, which was made as a last resort due to the previous methods failing. This happened due to a >>lack of permission<< for you to use Euler Stream's (https://www.eulerstream.com) fallback method. If you do not want to use Euler Stream, disable this fallback method by setting `RoomIdRouteConfig.skipFetchRoomIdFromEulerRoute` to `true`.");
		if (!response.ok) throw new InvalidResponseError({ routeId }, `Failed to retrieve Room ID from Euler due to an error: ${response.message}`);
		if (!response.room_id) throw new InvalidResponseError({ routeId }, "Failed to extract Room ID from Euler.");
		return response.room_id;
	} catch (err) {
		errors.push(err);
	}
	throw new InvalidResponseCompositeError({
		routeId,
		requestErrs: errors
	}, "Failed to retrieve Room ID from all sources.");
});
//#endregion
//#region src/lib/web/routes/composite/fetch-is-live.ts
const IsLiveRouteConfig = {
	skipFetchRoomInfoFromHtmlRoute: false,
	skipFetchRoomInfoFromApiLiveRoute: false,
	skipFetchRoomIdFromEulerRoute: false
};
/**
* Determines whether a user is currently live, falling back progressively:
*   1. HTML scrape (SIGI_STATE status field)
*   2. TikTok API Live endpoint (liveRoom.status)
*   3. Euler Stream (is_live flag — skipped if `disableEulerFallback` is true)
*
* Errors from each source are accumulated and surfaced via `FetchIsLiveError` if every source fails.
*/
const fetchIsLiveComposite = createRoute("fetchIsLiveRoute", async ({ routeId, uniqueId, webClient, apiClient }) => {
	const errors = [];
	const isOnline = (status) => status !== 4;
	if (!IsLiveRouteConfig.skipFetchRoomInfoFromHtmlRoute) try {
		const htmlStatus = (await fetchRoomInfoFromHtmlRoute({
			webClient,
			uniqueId
		}))?.liveRoom?.status;
		if (htmlStatus === void 0) throw new InvalidResponseError({ routeId }, "Failed to extract status from HTML.");
		return isOnline(htmlStatus);
	} catch (ex) {
		errors.push(ex);
	}
	if (!IsLiveRouteConfig.skipFetchRoomInfoFromApiLiveRoute) try {
		const roomData = await fetchRoomInfoFromApiLiveRoute({
			webClient,
			uniqueId
		});
		if (roomData?.data?.liveRoom?.status === void 0) throw new InvalidResponseError({ routeId }, "Failed to extract status from API.");
		return isOnline(roomData.data.liveRoom.status);
	} catch (err) {
		errors.push(err);
	}
	if (!IsLiveRouteConfig.skipFetchRoomIdFromEulerRoute) try {
		const response = await fetchRoomIdFromEulerRoute({
			webClient,
			apiClient,
			uniqueId
		});
		if (response.code !== 200) throw new InvalidResponseError({ routeId }, "Failed to extract status from Euler.");
		if (response.is_live === void 0) throw new InvalidResponseError({ routeId }, "Failed to extract is_live from Euler.");
		return response.is_live;
	} catch (err) {
		errors.push(err);
	}
	throw new InvalidResponseCompositeError({
		routeId,
		requestErrs: errors
	}, "Failed to retrieve live status from all sources.");
});
//#endregion
//#region src/lib/web/lib/device-presets.ts
const Locations = [
	{
		"lang_country": "en-GB",
		"lang": "en",
		"country": "GB",
		"tz_name": "Europe/London"
	},
	{
		"lang_country": "en-CA",
		"lang": "en",
		"country": "CA",
		"tz_name": "America/Toronto"
	},
	{
		"lang_country": "en-AU",
		"lang": "en",
		"country": "AU",
		"tz_name": "Australia/Sydney"
	},
	{
		"lang_country": "en-NZ",
		"lang": "en",
		"country": "NZ",
		"tz_name": "Pacific/Auckland"
	},
	{
		"lang_country": "en-ZA",
		"lang": "en",
		"country": "ZA",
		"tz_name": "Africa/Johannesburg"
	},
	{
		"lang_country": "en-IE",
		"lang": "en",
		"country": "IE",
		"tz_name": "Europe/Dublin"
	},
	{
		"lang_country": "en-JM",
		"lang": "en",
		"country": "JM",
		"tz_name": "America/Jamaica"
	},
	{
		"lang_country": "en-BZ",
		"lang": "en",
		"country": "BZ",
		"tz_name": "America/Belize"
	},
	{
		"lang_country": "en-TT",
		"lang": "en",
		"country": "TT",
		"tz_name": "America/Port_of_Spain"
	}
];
const Screens = [
	{
		"screen_width": 1920,
		"screen_height": 1080
	},
	{
		"screen_width": 2560,
		"screen_height": 1440
	},
	{
		"screen_width": 3840,
		"screen_height": 2160
	},
	{
		"screen_width": 4096,
		"screen_height": 2160
	},
	{
		"screen_width": 5120,
		"screen_height": 2880
	},
	{
		"screen_width": 7680,
		"screen_height": 4320
	},
	{
		"screen_width": 1152,
		"screen_height": 2048
	},
	{
		"screen_width": 1440,
		"screen_height": 2560
	},
	{
		"screen_width": 2160,
		"screen_height": 3840
	},
	{
		"screen_width": 4320,
		"screen_height": 7680
	}
];
const UserAgents = [
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 14.7; rv:130.0) Gecko/20100101 Firefox/130.0",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/128.0.2739.79",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/128.0.2739.79",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 OPR/113.0.0.0",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 OPR/113.0.0.0"
];
function userAgentToDevicePreset(userAgent) {
	const firstSlash = userAgent.indexOf("/");
	return {
		user_agent: userAgent,
		browser_name: userAgent.substring(0, firstSlash),
		browser_version: userAgent.substring(firstSlash + 1),
		browser_platform: userAgent.includes("Macintosh") ? "MacIntel" : "Win32",
		os: userAgent.includes("Macintosh") ? "mac" : "windows"
	};
}
function getRandomDevicePreset() {
	return process.env.RANDOMIZE_TIKTOK_DEVICE?.toLowerCase() === "true" ? Devices[Math.floor(Math.random() * Devices.length)] : Devices[6];
}
function getRandomLocationPreset() {
	return process.env.RANDOMIZE_TIKTOK_LOCATION?.toLowerCase() === "true" ? Locations[Math.floor(Math.random() * Locations.length)] : {
		lang_country: "en-DE",
		lang: "en",
		country: "DE",
		tz_name: "Europe/Berlin"
	};
}
function getRandomScreenPreset() {
	return process.env.RANDOMIZE_TIKTOK_SCREEN?.toLowerCase() === "true" ? Screens[Math.floor(Math.random() * Screens.length)] : {
		screen_width: 1920,
		screen_height: 1080
	};
}
function generateDeviceId() {
	let digits = "";
	for (let i = 0; i < 19; i++) digits += Math.floor(Math.random() * 10);
	return digits;
}
const Devices = UserAgents.map((userAgent) => userAgentToDevicePreset(userAgent));
//#endregion
//#region src/lib/web/config.ts
/**
* Global route registry. Call sites should read handlers from here (e.g. RouteConfig.fetchRoomInfo(...))
* rather than importing the route functions directly, so downstream consumers can swap implementations:
*
*   import { RouteConfig } from 'tiktok-live-connector';
*   RouteConfig.fetchRoomInfo = async ({ webClient, roomId }) => { ... };
*
*/
const RouteConfig = {
	fetchRoomGifts: fetchRoomGiftsRoute,
	fetchRoomInfo: fetchRoomInfoRoute,
	fetchRoomInfoFromApiLive: fetchRoomInfoFromApiLiveRoute,
	fetchRoomInfoFromHtml: fetchRoomInfoFromHtmlRoute,
	fetchRoomIdFromProvider: fetchRoomIdFromEulerRoute,
	fetchRoomInfoFromProvider: fetchRoomInfoFromEulerRoute,
	fetchSignedWebSocketFromProvider: fetchSignedWebSocketFromEulerRoute,
	fetchWebcastSignatureFromProvider: fetchWebcastSignatureFromEulerRoute,
	sendRoomChatFromProvider: sendRoomChatFromEulerRoute,
	fetchRoomIdComposite,
	fetchIsLiveComposite
};
const getWebConfig = ({ device, location, screen }) => {
	const baseConfig = structuredClone(WebcastWebConfigDefaults);
	baseConfig.DEFAULT_HTTP_CLIENT_PARAMS.app_language = location.lang;
	baseConfig.DEFAULT_HTTP_CLIENT_PARAMS.browser_language = location.lang_country;
	baseConfig.DEFAULT_HTTP_CLIENT_PARAMS.browser_name = device.browser_name;
	baseConfig.DEFAULT_HTTP_CLIENT_PARAMS.browser_platform = device.browser_platform;
	baseConfig.DEFAULT_HTTP_CLIENT_PARAMS.browser_version = device.browser_version;
	baseConfig.DEFAULT_HTTP_CLIENT_PARAMS.screen_height = screen.screen_height.toString();
	baseConfig.DEFAULT_HTTP_CLIENT_PARAMS.screen_width = screen.screen_width.toString();
	baseConfig.DEFAULT_HTTP_CLIENT_PARAMS.tz_name = location.tz_name;
	baseConfig.DEFAULT_HTTP_CLIENT_PARAMS.os = device.os;
	baseConfig.DEFAULT_HTTP_CLIENT_PARAMS.priority_region = location.country;
	baseConfig.DEFAULT_HTTP_CLIENT_PARAMS.region = location.country;
	baseConfig.DEFAULT_HTTP_CLIENT_PARAMS.webcast_language = location.lang;
	baseConfig.DEFAULT_HTTP_CLIENT_PARAMS.device_id = generateDeviceId();
	baseConfig.DEFAULT_HTTP_CLIENT_HEADERS["User-Agent"] = device.user_agent;
	return baseConfig;
};
const getRandomPresets = () => {
	return {
		device: getRandomDevicePreset(),
		screen: getRandomScreenPreset(),
		location: getRandomLocationPreset()
	};
};
//#endregion
//#region src/lib/web/lib/http-client.ts
var WebcastHttpClient = class {
	oAuthSessionBundle = null;
	_gotInstance;
	_eulerApiInstance;
	_webcastWebConfig;
	clientParams;
	clientHeaders;
	cookieJar;
	/**
	* Instantiate a Webcast HTTP Client
	*
	* @param webcastWebConfig Randomized config for Webcast requests
	* @param eulerApiInstance Rarely needed override for the cached & lazy-loaded API client
	* @param gotHttpConfig Rarely needed override for the Got HTTP library configuration
	*/
	constructor(webcastWebConfig, eulerApiInstance, gotHttpConfig) {
		const { headers: userHeaders, searchParams: userSearchParams, timeout, ...gotOptions } = gotHttpConfig || {};
		const cleanWebcastConfig = structuredClone(webcastWebConfig);
		cleanWebcastConfig.DEFAULT_HTTP_CLIENT_PARAMS = {
			...cleanWebcastConfig.DEFAULT_HTTP_CLIENT_PARAMS,
			...userSearchParams
		};
		cleanWebcastConfig.DEFAULT_HTTP_CLIENT_HEADERS = {
			...cleanWebcastConfig.DEFAULT_HTTP_CLIENT_HEADERS,
			...userHeaders
		};
		this.clientParams = cleanWebcastConfig.DEFAULT_HTTP_CLIENT_PARAMS;
		this.clientHeaders = cleanWebcastConfig.DEFAULT_HTTP_CLIENT_HEADERS;
		this.cookieJar = new WebcastCookieJar(webcastWebConfig);
		this._eulerApiInstance = eulerApiInstance ?? createEulerClient();
		this._webcastWebConfig = cleanWebcastConfig;
		this._gotInstance = got.extend({
			timeout: { request: parseInt(process.env.TIKTOK_CLIENT_TIMEOUT || "10000") },
			...gotOptions,
			searchParams: void 0,
			headers: void 0,
			cookieJar: this.cookieJar
		});
	}
	/**
	* Get an instance of the underlying API Client
	*/
	get apiClient() {
		return this._eulerApiInstance;
	}
	/**
	* Set the Room ID for the client
	*
	* @param roomId The client's Room ID
	*/
	set roomId(roomId) {
		this.clientParams.room_id = roomId;
	}
	/**
	* Get the Room ID for the client
	*/
	get roomId() {
		return this.clientParams.room_id || "";
	}
	/**
	* Issue an HTTP request, merging the per-call options with the live client state
	* (headers, search params, cookie jar) and optionally routing the URL through the
	* sign server first.
	*
	* @param options.host Host portion of the request URL.
	* @param options.path Path appended to the host.
	* @param options.searchParams Per-call query parameters. Merged on top of `clientParams`; per-call values win on key collision.
	* @param options.signRequest If true, the URL is signed via `RouteConfig.fetchWebcastSignatureFromProvider` and any returned signed URL / User-Agent are honored.
	* @param options.method HTTP method. Defaults to `GET`.
	* @param options.headers Per-call headers. Merged on top of `clientHeaders`; per-call values win on key collision.
	* @param options Any other field is forwarded to `got` as-is (body, responseType, retry, etc.).
	*/
	async request({ host, path, searchParams, signRequest, method = "GET", headers, ...extraOptions }) {
		const mergedHeaders = {
			...this.clientHeaders,
			...headers
		};
		const mergedParams = {
			...this.clientParams,
			...searchParams
		};
		let url = `${this.getBaseUrl(host)}/${path}?${new URLSearchParams(mergedParams)}`;
		if (signRequest) {
			let signResponse = await RouteConfig.fetchWebcastSignatureFromProvider({
				url,
				method: method.toString().toUpperCase(),
				userAgent: mergedHeaders["User-Agent"],
				webClient: this,
				apiClient: this._eulerApiInstance
			});
			if (signResponse.response?.signedUrl) {
				url = signResponse.response.signedUrl;
				if (signResponse.response.userAgent) mergedHeaders["User-Agent"] = signResponse.response.userAgent;
			}
		}
		return this._gotInstance(url, {
			headers: mergedHeaders,
			method,
			...extraOptions
		});
	}
	/**
	* Get HTML from TikTok website
	*
	* @param path Path to the HTML page
	* @param options Additional request options
	*/
	async getHtmlFromTikTokWebsite(path, options = {}) {
		return (await this.request({
			host: this._webcastWebConfig.TIKTOK_HOST_WEB,
			path,
			responseType: "text",
			signRequest: false,
			...options
		})).body;
	}
	/**
	* Get deserialized object from Webcast API
	*
	* @param path Path to the API endpoint
	* @param params Query parameters to be sent with the request
	* @param schemaName Schema name for deserialization
	* @param signRequest Whether to sign the request or not
	* @param options Additional request options
	*/
	async getDeserializedObjectFromWebcastApi(path, params, schemaName, signRequest = false, options = {}) {
		return deserializeMessage(schemaName, (await this.request({
			host: this._webcastWebConfig.TIKTOK_HOST_WEBCAST,
			path: "webcast/" + path,
			searchParams: params,
			signRequest,
			responseType: "buffer",
			...options
		})).body);
	}
	async postJsonObjectToWebcastApi(path, params, data, signRequest = false, options = {}) {
		return (await this.request({
			host: this._webcastWebConfig.TIKTOK_HOST_WEBCAST,
			path: "webcast/" + path,
			body: JSON.stringify(data),
			searchParams: params,
			responseType: "json",
			signRequest,
			method: "POST",
			...options,
			headers: {
				"Content-Type": "application/json; charset=UTF-8",
				...options.headers
			}
		})).body;
	}
	/**
	* Get JSON object from Webcast API
	*
	* @param path Path to the API endpoint
	* @param params Query parameters to be sent with the request
	* @param signRequest Whether to sign the request or not
	* @param options Additional request options
	*/
	async getJsonObjectFromWebcastApi(path, params, signRequest = false, options = {}) {
		return (await this.request({
			host: this._webcastWebConfig.TIKTOK_HOST_WEBCAST,
			path: "webcast/" + path,
			searchParams: params,
			responseType: "json",
			signRequest,
			...options
		})).body;
	}
	/**
	* Get JSON object from TikTok API
	*
	* @param path Path to the API endpoint
	* @param params Query parameters to be sent with the request
	* @param signRequest Whether to sign the request or not
	* @param options Additional request options
	*/
	async getJsonObjectFromTikTokApi(path, params, signRequest = false, options = {}) {
		return (await this.request({
			host: this._webcastWebConfig.TIKTOK_HOST_WEB,
			path,
			searchParams: params,
			responseType: "json",
			signRequest,
			...options
		})).body;
	}
	/**
	* Determine if a host is secure
	*
	* @param host The host to check
	* @protected
	*/
	isSecure(host) {
		const { hostname } = new URL(host.includes("://") ? host : `http://${host}`);
		return hostname === "localhost" || hostname === "[::1]" || hostname === "::1" || isIP(hostname) === 4 && hostname.startsWith("127.");
	}
	/**
	* Get the base URL from a host, no trailing slash
	*
	* @param host The host to get a base URL for
	* @protected
	*/
	getBaseUrl(host) {
		return `http${this.isSecure(host) ? "s" : ""}://${host}`;
	}
};
//#endregion
//#region src/lib/client/utilities.ts
function HandleError(errMsg, exceptionWrapper) {
	return function(originalMethod, _context) {
		return function(...args) {
			const handle = (err) => {
				if (exceptionWrapper) err = new exceptionWrapper(err, errMsg);
				this.handleError(err, errMsg);
			};
			try {
				const result = originalMethod.apply(this, args);
				if (result instanceof Promise) return result.catch((err) => {
					handle(err);
					throw err;
				});
				return result;
			} catch (err) {
				handle(err);
				throw err;
			}
		};
	};
}
//#endregion
//#region src/lib/client/index.ts
var TikTokLiveConnection = class extends EventEmitter {
	options;
	_webClient;
	_wsClientInstance = null;
	_wsClientProvider;
	_devicePresets;
	_roomInfo = null;
	_availableGifts = null;
	_connectState = "DISCONNECTED";
	/**
	* Create a new TikTokLiveConnection instance.
	*
	* @param uniqueId TikTok username (with or without leading `@`, or a full `https://www.tiktok.com/@user/live` URL).
	* @param options Connection options.
	* @param options.signApiKey Optional Euler Stream API key. If provided, written to the global `SignConfig.apiKey` before the underlying Euler client is created. Ignored when `eulerApiInstance` is also passed.
	* @param options.eulerApiInstance Optional pre-built Euler Stream API client. Takes precedence over `signApiKey`.
	* @param options.session Authenticated session bundle. Pass `session.cookie` to seed the cookie jar with `sessionid` and `tt-target-idc`, and/or `session.oAuthToken` to send an OAuth token to the sign server. Required when `authenticateWs` or `useMobile` is true.
	* @param options.authenticateWs Forward the session cookies / OAuth token to the sign server so the WebSocket is authenticated. Defaults to false.
	* @param options.useMobile Use the mobile WebSocket flow. Implies `authenticateWs: true` and requires `session.cookie`. Defaults to false.
	* @param options.processInitialData Decode and emit the message batch returned in the initial sign response. Defaults to true.
	* @param options.fetchRoomInfoOnConnect Fetch room info during connect, throwing `UserOfflineError` if the streamer is not live. Defaults to true.
	* @param options.enableExtendedGiftInfo Fetch the room gift list during connect so `WebcastGiftMessage` events carry an `extendedGiftInfo` field. Defaults to false.
	* @param options.clientPresets Pre-built device, screen, and location presets. Defaults to a freshly randomized set from `getRandomPresets()`.
	* @param options.webClientOptions Extra `got` options applied to the HTTP client (proxy agent, timeouts, etc.). Headers and search params from this object are merged with the defaults; transport-only fields are passed through.
	* @param options.wsClientOptions Extra `ws` options applied to the WebSocket client.
	* @param options.webConfigOverrides Partial overrides for the resolved `WebcastWebConfigDefaults` used by the HTTP client.
	* @param options.wsConfigOverrides Partial overrides for the resolved `WebcastWebSocketConfigDefaults` used by the WebSocket client.
	*/
	constructor(uniqueId, options) {
		super();
		this.uniqueId = uniqueId;
		this.uniqueId = validateAndNormalizeUniqueId(uniqueId);
		this.options = {
			processInitialData: options.processInitialData ?? true,
			fetchRoomInfoOnConnect: options.fetchRoomInfoOnConnect ?? true,
			enableExtendedGiftInfo: options.enableExtendedGiftInfo ?? false,
			authenticateWs: options.authenticateWs,
			useMobile: options.useMobile
		};
		if (options.signApiKey) SignConfig.apiKey = options.signApiKey;
		this._devicePresets = options.clientPresets ?? getRandomPresets();
		const webcastWebConfig = {
			...getWebConfig(this._devicePresets),
			...options.webConfigOverrides
		};
		if (options.session?.cookie) webcastWebConfig.DEFAULT_HTTP_CLIENT_HEADERS["Cookie"] = WebcastCookieJar.serializeCookieSessionBundle(webcastWebConfig, options.session.cookie);
		this._webClient = new WebcastHttpClient(webcastWebConfig, options.eulerApiInstance, options.webClientOptions);
		if (options.session?.oAuthToken) this._webClient.oAuthSessionBundle = options.session.oAuthToken;
		const webcastWebSocketConfig = {
			...getWebSocketConfigDefaults(this._devicePresets),
			...options.wsConfigOverrides
		};
		this._wsClientProvider = createWebSocketProvider(webcastWebSocketConfig, options.wsClientOptions);
		this.setDisconnected();
	}
	/**
	* Get the current Web Client instance
	*/
	get webClient() {
		return this._webClient;
	}
	/**
	* Get an instance of the underlying API Client
	*/
	get apiClient() {
		return this._webClient.apiClient;
	}
	/**
	* Get the current WebSocket client instance. Will be `null` if not connected.
	*/
	get wsClient() {
		return this._wsClientInstance;
	}
	/**
	* Set the connection state to disconnected
	* @protected
	*/
	setDisconnected() {
		this._connectState = "DISCONNECTED";
		this._roomInfo = null;
		this.clientParams.cursor = "";
		this.clientParams.room_id = "";
		this.clientParams.internal_ext = "";
	}
	/**
	* Get the current Room Info
	*/
	get roomInfo() {
		return this._roomInfo;
	}
	/**
	* Get the available gifts
	*/
	get availableGifts() {
		return this._availableGifts;
	}
	/**
	* Get the current connection state
	*/
	get isConnecting() {
		return this._connectState === "CONNECTING";
	}
	/**
	* Check if the connection is established
	*/
	get isConnected() {
		return this._connectState === "CONNECTED";
	}
	/**
	* Get the current client parameters
	*/
	get clientParams() {
		return this.webClient.clientParams;
	}
	/**
	* Get the current room ID
	*/
	get roomId() {
		return this.webClient.roomId;
	}
	/**
	* Get the current connection state including the cached room info and all available gifts
	* (if `enableExtendedGiftInfo` option enabled)
	*/
	get state() {
		return {
			isConnected: this.isConnected,
			isConnecting: this.isConnecting,
			roomId: this.roomId,
			roomInfo: this.roomInfo,
			availableGifts: this.availableGifts
		};
	}
	/**
	* Connects to the live stream of the specified streamer
	*
	* @param roomId Room ID to connect to. If not specified, the room ID will be retrieved from the TikTok API
	* @returns The current connection state
	*/
	async connect(roomId) {
		switch (this._connectState) {
			case "CONNECTED": throw new AlreadyConnectedError("Already connected!");
			case "CONNECTING": throw new AlreadyConnectingError("Already connecting!");
			default:
			case "DISCONNECTED": try {
				this._connectState = "CONNECTING";
				await this._connect(roomId);
				this._connectState = "CONNECTED";
				this.emit("connected", this.state);
				return this.state;
			} catch (err) {
				this.setDisconnected();
				this.handleError(err, "Error while connecting");
				throw err;
			}
		}
	}
	/**
	* Connects to the live stream of the specified streamer
	*
	* @param roomId Room ID to connect to. If not specified, the room ID will be retrieved from the TikTok API
	* @protected
	*/
	async _connect(roomId) {
		roomId ||= this.roomId || await this.fetchRoomId();
		this.webClient.roomId = roomId;
		if (this.options?.fetchRoomInfoOnConnect) {
			this._roomInfo = await this.fetchRoomInfo();
			if (this._roomInfo?.data?.status === 4) throw new UserOfflineError("The requested user isn't online :(");
		}
		if (this.options?.enableExtendedGiftInfo) this._availableGifts = await this.fetchAvailableGifts();
		let bundledOptions;
		if (this.options.useMobile) {
			const cookieBundle = await this.webClient.cookieJar.getSessionBundle();
			const oauthBundle = this.webClient.oAuthSessionBundle;
			if (!cookieBundle) throw new TypeError("Cookie Bundle could not be found.");
			bundledOptions = {
				useMobile: true,
				authenticateWs: true,
				session: {
					cookie: cookieBundle,
					oAuthToken: oauthBundle ?? void 0
				}
			};
		} else bundledOptions = { useMobile: false };
		const { fetchResult: protoMessageFetchResult, fetchResultCookieHeader, fetchResultRoomId } = await RouteConfig.fetchSignedWebSocketFromProvider({
			webClient: this._webClient,
			apiClient: this.apiClient,
			roomId,
			cursor: this.clientParams.cursor,
			...bundledOptions
		});
		await this.webClient.cookieJar.processSetCookieHeader(fetchResultCookieHeader);
		if (fetchResultRoomId) this.webClient.roomId = roomId = fetchResultRoomId;
		if (this.options?.processInitialData) await this.processProtoMessageFetchResult(protoMessageFetchResult);
		if (!protoMessageFetchResult.cursor) throw new InvalidResponseError({ routeId: "fetchSignedWebSocketFromEulerRoute" }, "Missing cursor in initial fetch response.");
		this.clientParams.cursor = protoMessageFetchResult.cursor;
		this.clientParams.internal_ext = protoMessageFetchResult.internalExt;
		const wsParams = {
			compress: "gzip",
			room_id: this.roomId,
			internal_ext: protoMessageFetchResult.internalExt,
			cursor: protoMessageFetchResult.cursor
		};
		for (const [key, value] of Object.entries(protoMessageFetchResult.wsParams || [])) if (value) wsParams[key] = value;
		return this.setupWebsocket(protoMessageFetchResult.wsUrl, wsParams, roomId);
	}
	/**
	* Disconnects the connection to the live stream
	*/
	async disconnect() {
		const wsClient = this._wsClientInstance;
		if (!this.isConnected || !wsClient) {
			this.setDisconnected();
			return;
		}
		await new Promise((resolve) => {
			wsClient.once("close", () => resolve());
			wsClient.close();
		});
	}
	/**
	* Fetch the room ID from the TikTok API
	*
	* @param uniqueId Optional unique ID to use instead of the current one
	*/
	async fetchRoomId(uniqueId = this.uniqueId) {
		this._webClient.roomId = await RouteConfig.fetchRoomIdComposite({
			uniqueId,
			webClient: this.webClient,
			apiClient: this.apiClient
		});
		return this._webClient.roomId;
	}
	/**
	* Fetch whether the streamer is currently live
	*/
	async fetchIsLive(uniqueId = this.uniqueId) {
		return RouteConfig.fetchIsLiveComposite({
			uniqueId,
			webClient: this.webClient,
			apiClient: this.apiClient
		});
	}
	/**
	* Get the current room info (including streamer info, room status and statistics)
	* @returns Promise that will be resolved when the room info has been retrieved from the API
	*/
	async fetchRoomInfo(roomId = this.roomId) {
		if (!roomId) roomId = await this.fetchRoomId();
		this._roomInfo = await RouteConfig.fetchRoomInfo({
			roomId,
			webClient: this.webClient
		});
		return this._roomInfo;
	}
	/**
	* Get the available gifts in the current room
	* @returns Promise that will be resolved when the available gifts have been retrieved from the API
	*/
	async fetchAvailableGifts() {
		return RouteConfig.fetchRoomGifts({
			roomId: this.roomId,
			webClient: this.webClient
		});
	}
	/**
	* Send a message to a TikTok LIVE Room
	*
	* @param content Message content to send to the stream
	* @param roomId Target room ID. If not specified, the message will be sent to the currently connected room. Note that a room ID is required to send a message, so if you're not currently connected to a room you must specify a room ID.
	*/
	async sendMessage(content, roomId = this.roomId) {
		return RouteConfig.sendRoomChatFromProvider({
			content,
			roomId,
			webClient: this.webClient,
			apiClient: this.apiClient
		});
	}
	/**
	* Set up the WebSocket connection
	*
	* @param wsUrl WebSocket URL
	* @param wsParams WebSocket parameters
	* @param roomId The room ID to connect to with the WebSocket client
	* @returns Promise that will be resolved when the WebSocket connection is established
	* @protected
	*/
	async setupWebsocket(wsUrl, wsParams, roomId) {
		const wsClient = this._wsClientInstance = this._wsClientProvider({
			roomId,
			baseUrl: wsUrl,
			wsParams,
			wsHeaders: { Cookie: await this.webClient.cookieJar.getCookieString() }
		});
		const connectPromise = new Promise((resolve, reject) => {
			const onConnectTimeout = () => {
				this._wsClientInstance?.close(1e3);
				if (this._wsClientInstance === wsClient) this._wsClientInstance = null;
				reject(new ConnectTimeoutError("Timed out whilst connecting to WebSocket"));
			};
			const onConnectError = (err) => {
				if (this._wsClientInstance === wsClient) this._wsClientInstance = null;
				clearTimeout(connectTimeout);
				reject(err);
			};
			const onConnectClose = () => {
				clearTimeout(connectTimeout);
				wsClient.removeAllListeners();
			};
			const onConnectOpen = () => {
				clearTimeout(connectTimeout);
				wsClient.removeListener("open", onConnectOpen);
				wsClient.removeListener("error", onConnectError);
				resolve();
			};
			const connectTimeout = setTimeout(onConnectTimeout, parseInt(process.env.WS_CONNECT_TIMEOUT_MS || "20000"));
			wsClient.once("error", onConnectError);
			wsClient.once("open", onConnectOpen);
			wsClient.once("close", onConnectClose);
		});
		wsClient.on("protoMessageFetchResult", this.processProtoMessageFetchResult.bind(this));
		wsClient.on("imEnteredRoom", (data) => this.emit("enterRoom", data));
		wsClient.on("webSocketData", (data) => this.emit("websocketData", data));
		wsClient.on("messageDecodingFailed", (err) => this.handleError(err, "Websocket message decoding failed"));
		await connectPromise;
		wsClient.on("error", (e) => {
			this.handleError(e, "WebSocket Error after connecting");
		});
		wsClient.on("close", (code, reason) => {
			wsClient.removeAllListeners();
			if (this._wsClientInstance === wsClient) this._wsClientInstance = null;
			this.setDisconnected();
			this.emit("disconnected", {
				code,
				reason: reason.toString()
			});
		});
		wsClient.switchRooms(roomId);
		this.emit("websocketConnected", wsClient);
	}
	async processProtoMessageFetchResult(protoMessageFetchResult) {
		for (const message of protoMessageFetchResult.messages) {
			if (!message.decodedData) continue;
			this.emit("decodedData", message.type, message.decodedData, message.payload);
			try {
				await this.processDecodedData(message.decodedData);
			} catch (ex) {
				this.handleError(ex, "Failed to process decoded data");
			}
		}
	}
	async processDecodedData({ data, type }) {
		switch (type) {
			case "WebcastSocialMessage":
				if (data.common?.displayText?.displayType?.includes("follow")) return this.emit("follow", data);
				if (data.common?.displayText?.displayType?.includes("share")) return this.emit("share", data);
				return this.emit("social", data);
			case "WebcastControlMessage":
				this.emit("controlMessage", data);
				if (data.action === ControlAction.CONTROL_ACTION_STREAM_ENDED || data.action === ControlAction.CONTROL_ACTION_STREAM_SUSPENDED) {
					this.emit("streamEnd", { action: data.action });
					await this.disconnect();
				}
				return;
			case "WebcastGiftMessage":
				if (Array.isArray(this.availableGifts) && data.giftId) data.extendedGiftInfo = this.availableGifts.find((x) => x.id === data.giftId);
				return this.emit("gift", data);
			case "WebcastBarrageMessage":
				if (data.content?.displayType?.toLowerCase().includes("ttlive_superfan")) this.emit("superFan", data);
				return this.emit("barrage", data);
			case "WebcastEnvelopeMessage":
				if (data.common?.displayText?.displayType?.toLowerCase().includes("ttlive_superfanbox") || data.envelopeInfo?.businessType === EnvelopeBusinessType.BusinessTypeSuperFanBox) this.emit("superFanBox", data);
				return this.emit("envelope", data);
			default:
				const basicEvent = WebcastEventMap[type];
				return basicEvent && this.emit(basicEvent, data);
		}
	}
	/**
	* Handle the error event
	*
	* @param exception Exception object
	* @param info Additional information about the error
	* @protected
	*/
	handleError(exception, info) {
		if (this.listenerCount("error") < 1) return;
		this.emit("error", {
			info,
			exception
		});
	}
	/**
	* Wait until the streamer is live
	*
	* @param seconds Number of seconds to wait before checking if the streamer is live again
	* @param abortSignal Process will keep checking if the streamer is live every `seconds` seconds until the streamer is live or the abort signal is triggered
	*
	*/
	async waitUntilLive(seconds = 60, abortSignal) {
		seconds = Math.max(30, seconds);
		return new Promise((resolve, reject) => {
			if (abortSignal?.aborted) return reject(abortSignal.reason);
			let timer;
			const cleanup = () => {
				if (timer !== void 0) clearTimeout(timer);
				abortSignal?.removeEventListener("abort", onAbort);
			};
			const onAbort = () => {
				cleanup();
				reject(abortSignal.reason);
			};
			const check = async () => {
				try {
					const isLive = await this.fetchIsLive();
					if (abortSignal?.aborted) return;
					if (isLive) {
						cleanup();
						resolve();
						return;
					}
					timer = setTimeout(check, seconds * 1e3);
				} catch (err) {
					if (abortSignal?.aborted) return;
					cleanup();
					reject(err);
				}
			};
			abortSignal?.addEventListener("abort", onAbort, { once: true });
			check();
		});
	}
};
function applyHandleError(methodName, errMsg) {
	const descriptor = Object.getOwnPropertyDescriptor(TikTokLiveConnection.prototype, methodName);
	if (!descriptor || typeof descriptor.value !== "function") throw new Error(`Cannot apply HandleError to ${methodName}`);
	Object.defineProperty(TikTokLiveConnection.prototype, methodName, {
		...descriptor,
		value: HandleError(errMsg)(descriptor.value, {})
	});
}
applyHandleError("fetchRoomId", "Failed to retrieve Room ID from all sources.");
applyHandleError("fetchIsLive", "Failed to retrieve live status from all sources.");
applyHandleError("fetchRoomInfo", "Failed to fetch room info.");
applyHandleError("fetchAvailableGifts", "Failed to fetch room gifts.");
applyHandleError("sendMessage", "Failed to send message.");
//#endregion
export { AuthenticatedWebSocketConnectionError as $, CompositeFetchRoute as A, WebcastEventMap as B, fetchRoomGiftsRoute as C, fetchRoomInfoFromEulerRoute as D, fetchSignedWebSocketFromEulerRoute as E, createEulerClient as F, WebcastWebConfigDefaults as G, generateUniqId as H, VERSION as I, deserializeMessage as J, WebcastDeserializeConfig as K, ConnectState as L, createRoute as M, LIBRARY_IDENTITY as N, fetchRoomIdFromEulerRoute as O, SignConfig as P, AlreadyConnectingError as Q, ControlEvent as R, fetchRoomInfoRoute as S, fetchWebcastSignatureFromEulerRoute as T, getWebSocketConfigDefaults as U, createWebSocketProvider as V, WebSocketConfigDefaults as W, validateAndNormalizeUniqueId as X, deserializeWebSocketMessage as Y, AlreadyConnectedError as Z, RoomIdRouteConfig as _, getWebConfig as a, InvalidSchemaNameError as at, fetchRoomInfoFromHtmlRoute as b, Screens as c, SchemaDecodeError as ct, getRandomDevicePreset as d, SignatureRateLimitError as dt, ConnectTimeoutError as et, getRandomLocationPreset as f, TikTokLiveError as ft, fetchIsLiveComposite as g, IsLiveRouteConfig as h, getRandomPresets as i, InvalidResponseError as it, EulerFetchRoute as j, BaseFetchRoute as k, UserAgents as l, SignAPIError as lt, userAgentToDevicePreset as m, HandleError as n, InvalidRequestError as nt, Devices as o, InvalidUniqueIdError as ot, getRandomScreenPreset as p, UserOfflineError as pt, createBaseWebcastPushFrame as q, RouteConfig as r, InvalidResponseCompositeError as rt, Locations as s, PremiumFeatureError as st, TikTokLiveConnection as t, ErrorReason as tt, generateDeviceId as u, SignatureMissingTokensError as ut, fetchRoomIdComposite as v, sendRoomChatFromEulerRoute as w, fetchRoomInfoFromApiLiveRoute as x, RoomInfoFromHtmlRouteConfig as y, WebcastEvent as z };

//# sourceMappingURL=lib-CZoAbYHv.js.map
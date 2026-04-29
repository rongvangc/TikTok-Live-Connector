import * as tikTokSchema from "tiktok-live-proto/v2";
import { ControlAction, MessageFns, ProtoMessageFetchResult, RoomVerifyMessage, WebcastBarrageMessage, WebcastCaptionMessage, WebcastChatMessage, WebcastControlMessage, WebcastEmoteChatMessage, WebcastEnvelopeMessage, WebcastGiftMessage, WebcastGoalUpdateMessage, WebcastHourlyRankMessage, WebcastImDeleteMessage, WebcastInRoomBannerMessage, WebcastLikeMessage, WebcastLinkLayerMessage, WebcastLinkMessage, WebcastLinkMicArmies, WebcastLinkMicBattle, WebcastLinkMicBattlePunishFinish, WebcastLinkMicFanTicketMethod, WebcastLinkMicMethod, WebcastLinkmicBattleTaskMessage, WebcastLiveIntroMessage, WebcastMemberMessage, WebcastMsgDetectMessage, WebcastOecLiveShoppingMessage, WebcastPollMessage, WebcastPushFrame, WebcastQuestionNewMessage, WebcastRankTextMessage, WebcastRankUpdateMessage, WebcastRoomMessage, WebcastRoomPinMessage, WebcastRoomUserSeqMessage, WebcastSocialMessage, WebcastSystemMessage, WebcastUnauthorizedMemberMessage } from "tiktok-live-proto/v2";
import { BinaryWriter } from "@bufbuild/protobuf/wire";
import { ClientOptions, WebSocket } from "ws";
import * as _$got from "got";
import { Got, OptionsInit, PromiseCookieJar } from "got";
import EulerStreamApiClient, { ClientConfiguration, SignTikTokUrlBodyMethodEnum, SignWebcastUrl200Response, SoaxProxyRegion, WebcastRoomChatRouteResponse, WebcastRoomIdRouteResponse, WebcastRoomInfoRouteResponse } from "@eulerstream/euler-api-sdk";
import TypedEventEmitter from "typed-emitter";
import { URL } from "url";

//#region src/types/web.d.ts
type LocationPreset = {
  lang: string;
  lang_country: string;
  country: string;
  tz_name: string;
};
type DevicePreset = {
  browser_version: string;
  browser_name: string;
  browser_platform: string;
  user_agent: string;
  os: string;
};
type ScreenPreset = {
  screen_width: number;
  screen_height: number;
};
type GetWebConfigParams = {
  screen: ScreenPreset;
  location: LocationPreset;
  device: DevicePreset;
};
type WebSocketDynamicParams = {
  roomId: string;
  baseUrl: string;
  wsParams: WebSocketParams;
  wsHeaders: Record<string, string>;
};
type GetWebSocketConfigParams = GetWebConfigParams;
interface AbstractWebcastCookieJar extends PromiseCookieJar {
  getCookieString: (url: string) => Promise<string>;
  setCookie: (rawCookie: string, url: string) => Promise<unknown>;
  processSetCookieHeader(setCookieHeader: unknown): Promise<void>;
  getSessionBundle(): Promise<CookieSessionBundle | null>;
  setSessionBundle(session: CookieSessionBundle): Promise<void>;
}
/**
 * Narrow type to only allow Record-based params
 */
type WebcastGotHttpConfig = Omit<OptionsInit, 'searchParams' | 'cookieJar'> & {
  searchParams?: Record<string, string>;
  cookieJar: AbstractWebcastCookieJar;
};
//#endregion
//#region src/lib/ws/defaults.d.ts
declare const WebSocketConfigDefaults: {
  TIKTOK_HOST_WS: string;
  DEFAULT_WS_CLIENT_PARAMS: {
    version_code: string;
    aid: string;
    app_language: string;
    app_name: string;
    browser_platform: string;
    browser_language: string;
    browser_name: string;
    browser_version: string;
    browser_online: string;
    cookie_enabled: string;
    tz_name: string;
    device_platform: string;
    identity: string;
    live_id: string;
    webcast_language: string;
    ws_direct: string;
    sup_ws_ds_opt: string;
    update_version_code: string;
    did_rule: string;
    screen_height: string;
    screen_width: string;
    heartbeat_duration: string;
    resp_content_type: string;
    history_comment_count: string;
    client_enter: string;
    last_rtt: string;
  };
  DEFAULT_WS_CLIENT_PARAMS_APPEND_PARAMETER: string;
  DEFAULT_WS_CLIENT_HEADERS: {
    'User-Agent': string;
  };
  DEFAULT_WS_PING_INTERVAL: number;
};
type WebcastWebSocketConfigDefaults = typeof WebSocketConfigDefaults;
type WebcastWebSocketConfig = WebcastWebSocketConfigDefaults & WebSocketDynamicParams;
//#endregion
//#region src/lib/ws/config.d.ts
declare function getWebSocketConfigDefaults({
  device,
  screen,
  location
}: GetWebSocketConfigParams): WebcastWebSocketConfigDefaults;
//#endregion
//#region src/lib/ws/lib/proto-utils.d.ts
declare const WebcastDeserializeConfig: IWebcastDeserializeConfig;
declare function deserializeMessage<T extends keyof WebcastMessage>(protoName: T, binaryMessage: Buffer): WebcastMessage[T];
declare function deserializeWebSocketMessage(binaryMessage: Uint8Array): Promise<DecodedWebcastPushFrame>;
/**
 * Validates and normalizes the uniqueId (username) input for TikTok live room information retrieval.
 * It ensures that the input is a string and extracts the username from various possible formats, such as full URLs or with/without '@' symbol. If the input is invalid, it throws an error with a descriptive message.
 *
 * @param uniqueId Input value representing the unique identifier (username) of a TikTok user. This can be in various formats, such as a plain username, a full TikTok URL, or with an '@' symbol.
 */
declare function validateAndNormalizeUniqueId(uniqueId: unknown): string;
/**
 * Create a base WebcastPushFrame with default values, allowing overrides for specific fields. This is useful for testing or constructing messages without needing to specify every field.
 *
 * @param overrides Overrides to apply to the default WebcastPushFrame. Fields not included in the overrides will be set to default values that typically indicate "not set" (e.g., "0" for numeric fields, empty buffer for payload).
 */
declare function createBaseWebcastPushFrame(overrides: Partial<WebcastPushFrame>): BinaryWriter;
//#endregion
//#region src/lib/ws/lib/ws-utils.d.ts
/**
 * Generates a random java.Long for the uniqId string.
 */
declare function generateUniqId(): string;
//#endregion
//#region src/lib/web/defaults.d.ts
declare const WebcastWebConfigDefaults: {
  TIKTOK_HOST_WEB: string;
  TIKTOK_HOST_WEBCAST: string;
  TIKTOK_HTTP_ORIGIN: string;
  DEFAULT_HTTP_CLIENT_OPTIONS: {};
  DEFAULT_HTTP_CLIENT_HEADERS: {
    Cookie: string;
    Connection: string;
    'Cache-Control': string;
    'User-Agent': string;
    Accept: string;
    Referer: string;
    Origin: string;
    'Accept-Language': string;
    'Accept-Encoding': string;
    'Sec-Fetch-Site': string;
    'Sec-Fetch-Mode': string;
    'Sec-Fetch-Dest': string;
    'Sec-Fetch-Ua-Mobile': string;
  };
  DEFAULT_HTTP_CLIENT_PARAMS: {
    aid: string;
    app_language: string;
    app_name: string;
    browser_language: string;
    browser_name: string;
    browser_online: string;
    browser_platform: string;
    browser_version: string;
    cookie_enabled: string;
    device_platform: string;
    focus_state: string;
    from_page: string;
    history_len: string;
    is_fullscreen: string;
    is_page_visible: string;
    screen_height: string;
    screen_width: string;
    tz_name: string;
    referer: string;
    root_referer: string;
    channel: string;
    data_collection_enabled: string;
    os: string;
    priority_region: string;
    region: string;
    user_is_login: string;
    webcast_language: string;
    device_id: string;
  };
  SESSION_COOKIE_NAMES: string[];
  TARGET_IDC_COOKIE_NAME: string;
};
type WebcastWebConfigDefaults = typeof WebcastWebConfigDefaults;
//#endregion
//#region src/types/client.d.ts
type CookieSessionBundle = {
  type: 'cookie';
  value: {
    ttTargetIdc: string;
    sessionId: string;
  };
};
type OAuthTokenSessionBundle = {
  type: 'oAuthToken';
  value: string;
};
type SessionBundle = {
  cookie: CookieSessionBundle;
  oAuthToken: OAuthTokenSessionBundle;
};
type SessionBundleWithCookie = Omit<Partial<SessionBundle>, 'cookie'> & {
  cookie: SessionBundle['cookie'];
};
type TikTokLiveConnectionBundledAuthOptions = {
  useMobile: true;
  session: SessionBundleWithCookie;
  authenticateWs: true;
} | {
  useMobile?: false;
  session: SessionBundleWithCookie;
  authenticateWs?: true;
} | {
  useMobile?: false;
  session?: Partial<SessionBundle>;
  authenticateWs?: false;
};
type TikTokLiveConnectionProviderOptions = {
  /**
   * Ignored if `eulerApiInstance` is provided. If not, an instance will be created with this API key (or the one from env vars if not provided).
   */
  signApiKey?: string;
  /**
   * Pass an existing instance of the Euler Stream API client to be used for all requests to Euler's endpoints. Or use SignConfig.
   */
  eulerApiInstance?: EulerStreamApiClient;
} & TikTokLiveConnectionBundledAuthOptions;
type TikTokLiveConnectionOptions = TikTokLiveConnectionProviderOptions & {
  processInitialData: boolean;
  fetchRoomInfoOnConnect: boolean;
  enableExtendedGiftInfo: boolean;
  clientPresets?: GetWebConfigParams;
  webClientOptions?: WebcastGotHttpConfig;
  wsClientOptions?: ClientOptions;
  webConfigOverrides?: Partial<WebcastWebConfigDefaults>;
  wsConfigOverrides?: Partial<WebcastWebSocketConfigDefaults>;
};
type TikTokLiveConstructorConnectionOptions = Partial<TikTokLiveConnectionOptions> & TikTokLiveConnectionProviderOptions;
type TikTokLiveConnectionMutableOptions = Pick<TikTokLiveConnectionOptions, 'processInitialData' | 'fetchRoomInfoOnConnect' | 'enableExtendedGiftInfo' | 'authenticateWs' | 'useMobile'>;
type RoomInfo = Record<string, any> & {
  data: {
    status: number;
  };
};
type RoomGiftInfo = any;
type DecodedWebcastPushFrame = WebcastPushFrame & {
  protoMessageFetchResult?: ProtoMessageFetchResult;
};
type ExtractMessageType<T> = T extends MessageFns<infer U> ? U : never;
type WebcastMessage = { [K in keyof typeof tikTokSchema as ExtractMessageType<typeof tikTokSchema[K]> extends never ? never : K]: ExtractMessageType<typeof tikTokSchema[K]> };
type HasCommon<T> = T extends {
  common: any;
} ? T : never;
type WebcastEventMessage = { [K in keyof WebcastMessage as HasCommon<WebcastMessage[K]> extends never ? never : K]: WebcastMessage[K] };
interface IWebcastDeserializeConfig {
  skipMessageTypes: (keyof WebcastEventMessage)[];
  showBase64OnDecodeError: boolean;
}
interface IWebcastDeserializeConfig {
  skipMessageTypes: (keyof WebcastEventMessage)[];
}
type DecodedData = { [K in keyof WebcastEventMessage]: {
  type: K;
  data: WebcastEventMessage[K];
} }[keyof WebcastEventMessage];
declare module 'tiktok-live-proto/v2' {
  interface BaseProtoMessage {
    decodedData?: DecodedData;
    decodeError?: any;
  }
  interface WebcastGiftMessage {
    extendedGiftInfo?: any;
  }
}
type WebcastHttpClientRequestParams = Omit<OptionsInit, 'url' | 'prefixUrl' | 'searchParams' | 'cookieJar' | 'headers'> & {
  host: string;
  path: string;
  headers?: Record<string, string>;
  searchParams?: Record<string, string>;
  signRequest: boolean;
};
type WebSocketParams = {
  [key: string]: string | undefined;
  compress?: string;
  room_id: string;
  internal_ext: string;
  cursor: string;
};
//#endregion
//#region src/types/ws.d.ts
type WebcastWebSocketEventMap = {
  close: () => void;
  messageDecodingFailed: (error: Error) => void;
  protoMessageFetchResult: (response: any) => void;
  webSocketData: (data: Buffer) => void;
  imEnteredRoom: (decodedContainer: DecodedWebcastPushFrame) => void;
};
type WebcastTypedWebSocket = new (...args: any[]) => WebSocket & TypedEventEmitter<WebcastWebSocketEventMap>;
//#endregion
//#region src/lib/ws/lib/ws-client.d.ts
/**
 * Creates a WebSocket provider function that can be used to create WebcastWebSocketClient instances with dynamic parameters.
 *
 * @param webcastWebSocketConfig The default WebSocket configuration to use for all clients created by this provider. Dynamic parameters will override these defaults.
 * @param wsClientOptions The WebSocket client options to use for all clients created by this provider. These options will be merged with any dynamic parameters provided when creating a client.
 *
 */
declare function createWebSocketProvider(webcastWebSocketConfig: WebcastWebSocketConfigDefaults, wsClientOptions?: ClientOptions): (dynamicParams: WebSocketDynamicParams) => WebcastWebSocketClient;
declare const WebcastWebSocketClient_base: WebcastTypedWebSocket;
declare class WebcastWebSocketClient extends WebcastWebSocketClient_base {
  protected pingInterval: NodeJS.Timeout | null;
  protected seqId: number;
  protected enterUniqueId: string | null;
  protected roomId: string;
  protected webcastWsConfig: WebcastWebSocketConfig;
  constructor(webcastWebSocketConfig: WebcastWebSocketConfig, wsClientOptions?: ClientOptions);
  get open(): boolean;
  /**
   * Send a message to the WebSocket server
   * @param data The message to send
   * @returns True if the message was sent, false otherwise
   */
  sendBytes(data: Uint8Array): boolean;
  protected onDisconnect(): void;
  /**
   * Handle incoming messages
   * @param message The incoming WebSocket message (type => Buffer)
   * @protected
   */
  protected onMessage(message: Buffer): Promise<void>;
  /**
   * Static Keep-Alive ping
   */
  protected sendHeartbeat(): void;
  /**
   * Switch to a different TikTok LIVE room while connected to the WebSocket
   *
   * @param roomId The room ID to switch to
   */
  switchRooms(roomId: string): void;
  /**
   * Acknowledge the message was received
   */
  protected sendAck({
    logId,
    protoMessageFetchResult
  }: DecodedWebcastPushFrame): void;
}
//#endregion
//#region src/types/events.d.ts
declare enum ControlEvent {
  CONNECTED = "connected",
  DISCONNECTED = "disconnected",
  ERROR = "error",
  RAW_DATA = "rawData",
  DECODED_DATA = "decodedData",
  WEBSOCKET_CONNECTED = "websocketConnected",
  WEBSOCKET_DATA = "websocketData",
  ENTER_ROOM = "enterRoom"
}
declare enum WebcastEvent {
  CHAT = "chat",
  MEMBER = "member",
  GIFT = "gift",
  ROOM_USER = "roomUser",
  SOCIAL = "social",
  LIKE = "like",
  QUESTION_NEW = "questionNew",
  LINK_MIC_BATTLE = "linkMicBattle",
  LINK_MIC_ARMIES = "linkMicArmies",
  LIVE_INTRO = "liveIntro",
  EMOTE = "emote",
  ENVELOPE = "envelope",
  FOLLOW = "follow",
  SHARE = "share",
  STREAM_END = "streamEnd",
  CONTROL_MESSAGE = "controlMessage",
  BARRAGE = "barrage",
  SYSTEM = "system",
  HOURLY_RANK = "hourlyRank",
  GOAL_UPDATE = "goalUpdate",
  ROOM_MESSAGE = "roomMessage",
  CAPTION_MESSAGE = "captionMessage",
  IM_DELETE = "imDelete",
  IN_ROOM_BANNER = "inRoomBanner",
  RANK_UPDATE = "rankUpdate",
  POLL_MESSAGE = "pollMessage",
  RANK_TEXT = "rankText",
  LINK_MIC_BATTLE_PUNISH_FINISH = "linkMicBattlePunishFinish",
  LINK_MIC_BATTLE_TASK = "linkMicBattleTask",
  LINK_MIC_FAN_TICKET_METHOD = "linkMicFanTicketMethod",
  LINK_MIC_METHOD = "linkMicMethod",
  UNAUTHORIZED_MEMBER = "unauthorizedMember",
  OEC_LIVE_SHOPPING = "oecLiveShopping",
  MSG_DETECT = "msgDetect",
  LINK_MESSAGE = "linkMessage",
  ROOM_VERIFY = "roomVerify",
  LINK_LAYER = "linkLayer",
  ROOM_PIN = "roomPin",
  SUPER_FAN = "superFan",
  SUPER_FAN_JOIN = "superFanJoin",
  SUPER_FAN_BOX = "superFanBox"
}
declare enum ConnectState {
  DISCONNECTED = "DISCONNECTED",
  CONNECTING = "CONNECTING",
  CONNECTED = "CONNECTED"
}
type EventHandler<T> = (event: T) => void | Promise<void>;
type ClientEventMap = {
  [WebcastEvent.FOLLOW]: EventHandler<WebcastSocialMessage>;
  [WebcastEvent.SUPER_FAN]: EventHandler<WebcastBarrageMessage>;
  [WebcastEvent.SUPER_FAN_JOIN]: EventHandler<WebcastBarrageMessage>;
  [WebcastEvent.SUPER_FAN_BOX]: EventHandler<WebcastEnvelopeMessage>;
  [WebcastEvent.SHARE]: EventHandler<WebcastSocialMessage>;
  [WebcastEvent.STREAM_END]: (event: {
    action: ControlAction;
  }) => void | Promise<void>;
  [ControlEvent.CONNECTED]: EventHandler<TikTokLiveConnectionState>;
  [ControlEvent.DISCONNECTED]: EventHandler<{
    code: number;
    reason?: string;
  }>;
  [ControlEvent.ERROR]: EventHandler<any>;
  [ControlEvent.WEBSOCKET_DATA]: EventHandler<Uint8Array>;
  [ControlEvent.RAW_DATA]: (type: string, data: Uint8Array) => void | Promise<void>;
  [ControlEvent.DECODED_DATA]: (type: string, event: any, binary: Uint8Array) => void | Promise<void>;
  [ControlEvent.WEBSOCKET_CONNECTED]: EventHandler<WebcastWebSocketClient>;
  [ControlEvent.ENTER_ROOM]: EventHandler<DecodedWebcastPushFrame>;
  [WebcastEvent.CHAT]: EventHandler<WebcastChatMessage>;
  [WebcastEvent.MEMBER]: EventHandler<WebcastMemberMessage>;
  [WebcastEvent.GIFT]: EventHandler<WebcastGiftMessage>;
  [WebcastEvent.ROOM_USER]: EventHandler<WebcastRoomUserSeqMessage>;
  [WebcastEvent.SOCIAL]: EventHandler<WebcastSocialMessage>;
  [WebcastEvent.LIKE]: EventHandler<WebcastLikeMessage>;
  [WebcastEvent.QUESTION_NEW]: EventHandler<WebcastQuestionNewMessage>;
  [WebcastEvent.LINK_MIC_BATTLE]: EventHandler<WebcastLinkMicBattle>;
  [WebcastEvent.LINK_MIC_ARMIES]: EventHandler<WebcastLinkMicArmies>;
  [WebcastEvent.LIVE_INTRO]: EventHandler<WebcastLiveIntroMessage>;
  [WebcastEvent.EMOTE]: EventHandler<WebcastEmoteChatMessage>;
  [WebcastEvent.ENVELOPE]: EventHandler<WebcastEnvelopeMessage>;
  [WebcastEvent.CONTROL_MESSAGE]: EventHandler<WebcastControlMessage>;
  [WebcastEvent.BARRAGE]: EventHandler<WebcastBarrageMessage>;
  [WebcastEvent.HOURLY_RANK]: EventHandler<WebcastHourlyRankMessage>;
  [WebcastEvent.SYSTEM]: EventHandler<WebcastSystemMessage>;
  [WebcastEvent.GOAL_UPDATE]: EventHandler<WebcastGoalUpdateMessage>;
  [WebcastEvent.ROOM_MESSAGE]: EventHandler<WebcastRoomMessage>;
  [WebcastEvent.CAPTION_MESSAGE]: EventHandler<WebcastCaptionMessage>;
  [WebcastEvent.IM_DELETE]: EventHandler<WebcastImDeleteMessage>;
  [WebcastEvent.IN_ROOM_BANNER]: EventHandler<WebcastInRoomBannerMessage>;
  [WebcastEvent.RANK_UPDATE]: EventHandler<WebcastRankUpdateMessage>;
  [WebcastEvent.POLL_MESSAGE]: EventHandler<WebcastPollMessage>;
  [WebcastEvent.RANK_TEXT]: EventHandler<WebcastRankTextMessage>;
  [WebcastEvent.LINK_MIC_BATTLE_PUNISH_FINISH]: EventHandler<WebcastLinkMicBattlePunishFinish>;
  [WebcastEvent.LINK_MIC_BATTLE_TASK]: EventHandler<WebcastLinkmicBattleTaskMessage>;
  [WebcastEvent.LINK_MIC_FAN_TICKET_METHOD]: EventHandler<WebcastLinkMicFanTicketMethod>;
  [WebcastEvent.LINK_MIC_METHOD]: EventHandler<WebcastLinkMicMethod>;
  [WebcastEvent.UNAUTHORIZED_MEMBER]: EventHandler<WebcastUnauthorizedMemberMessage>;
  [WebcastEvent.OEC_LIVE_SHOPPING]: EventHandler<WebcastOecLiveShoppingMessage>;
  [WebcastEvent.MSG_DETECT]: EventHandler<WebcastMsgDetectMessage>;
  [WebcastEvent.LINK_MESSAGE]: EventHandler<WebcastLinkMessage>;
  [WebcastEvent.ROOM_VERIFY]: EventHandler<RoomVerifyMessage>;
  [WebcastEvent.LINK_LAYER]: EventHandler<WebcastLinkLayerMessage>;
  [WebcastEvent.ROOM_PIN]: EventHandler<WebcastRoomPinMessage>;
};
declare const WebcastEventMap: Record<BasicWebcastEventMessage, keyof ClientEventMap>;
type BasicWebcastEventMessage = keyof Omit<WebcastEventMessage, 'WebcastGiftMessage'>;
type TikTokLiveConnectionState = {
  isConnected: boolean;
  isConnecting: boolean;
  roomId: string;
  roomInfo: RoomInfo | null;
  availableGifts: RoomGiftInfo | null;
};
type WebcastTypedClient = new () => TypedEventEmitter<ClientEventMap>;
//#endregion
//#region src/types/errors.d.ts
/**
 * Minimal structural type for the response object passed to {@link SignatureRateLimitError}.
 * Avoids depending on axios's declaration files (which the library no longer ships at runtime).
 */
type RateLimitedResponse = {
  headers: Record<string, string | undefined>;
};
declare class ConnectError extends Error {
  constructor(message: string);
}
declare class InvalidUniqueIdError extends Error {}
type InvalidResponseErrorConfig = {
  routeId: string;
  requestErr?: Error;
};
type InvalidRequestErrorConfig = {
  routeId: string;
};
declare class InvalidResponseError extends Error {
  readonly config: InvalidResponseErrorConfig;
  constructor(config: InvalidResponseErrorConfig, ...args: string[]);
}
type InvalidResponseCompositeErrorConfig = {
  routeId: string;
  requestErrs?: Error[];
};
declare class InvalidResponseCompositeError extends Error {
  readonly config: InvalidResponseCompositeErrorConfig;
  constructor(config: InvalidResponseCompositeErrorConfig, ...args: any[]);
}
declare class InvalidRequestError extends Error {
  readonly config: InvalidRequestErrorConfig;
  constructor(config: InvalidRequestErrorConfig, ...args: string[]);
}
declare class AlreadyConnectingError extends ConnectError {}
declare class AlreadyConnectedError extends ConnectError {}
declare class UserOfflineError extends ConnectError {}
declare class ConnectTimeoutError extends ConnectError {}
declare class InvalidSchemaNameError extends Error {}
declare class SchemaDecodeError extends Error {}
declare class TikTokLiveError extends Error {
  constructor(message: string);
}
declare enum ErrorReason {
  RATE_LIMIT = "Rate Limited",
  CONNECT_ERROR = "Connect Error",
  EMPTY_PAYLOAD = "Empty Payload",
  SIGN_NOT_200 = "Sign Error",
  EMPTY_COOKIES = "Empty Cookies",
  PREMIUM_FEATURE = "Premium Feature",
  AUTHENTICATED_WS = "Authenticated WS"
}
declare class SignAPIError extends TikTokLiveError {
  reason: ErrorReason;
  readonly requestId?: string;
  readonly agentId?: string;
  constructor(reason: ErrorReason, requestId?: string, agentId?: string, ...args: (string | Error | undefined)[]);
  static formatSignServerMessage(message: string): string;
}
declare class SignatureRateLimitError extends SignAPIError {
  readonly retryAfter: number;
  readonly resetTime?: number;
  constructor(apiMessage: string | undefined, formatStr: string, response: RateLimitedResponse);
  private static parseHeaderNumber;
  private static calculateRetryAfter;
  private static calculateResetTime;
}
declare class SignatureMissingTokensError extends SignAPIError {
  constructor(...args: string[]);
}
declare class PremiumFeatureError extends SignAPIError {
  constructor(apiMessage: string, ...args: string[]);
}
declare class AuthenticatedWebSocketConnectionError extends SignAPIError {
  constructor(...args: string[]);
}
//#endregion
//#region src/types/route.d.ts
/**
 * Per-call options forwarded to the Euler Stream SDK (axios-shaped under the hood).
 * Typed loosely so the public surface doesn't pull in axios's declaration files.
 */
type EulerRouteRequestOptions = Record<string, any>;
/**
 * Makes calls to TikTok directly
 */
type WebcastHttpRouteArgs<T = {}> = {
  webClient: WebcastHttpClient;
} & T;
/**
 * Makes calls to Euler Stream's API
 */
type WebcastHttpEulerRouteArgs<T = {}> = WebcastHttpRouteArgs & {
  apiClient: EulerStreamApiClient;
  options?: EulerRouteRequestOptions;
} & T;
/**
 * Combines the use of TikTok & Euler Stream APIs
 */
type WebcastHttpCompositeRouteArgs<T = {}> = WebcastHttpEulerRouteArgs<T>;
type HttpRoute<P, R> = (args: P) => Promise<R>;
type WrappedHttpRoute<P, R> = (args: P & {
  routeId: string;
}) => Promise<R>;
declare namespace index_d_exports {
  export { AbstractWebcastCookieJar, AlreadyConnectedError, AlreadyConnectingError, AuthenticatedWebSocketConnectionError, BasicWebcastEventMessage, ClientEventMap, ConnectState, ConnectTimeoutError, ControlEvent, CookieSessionBundle, DecodedData, DecodedWebcastPushFrame, DevicePreset, ErrorReason, EulerRouteRequestOptions, EventHandler, GetWebConfigParams, GetWebSocketConfigParams, HttpRoute, IWebcastDeserializeConfig, InvalidRequestError, InvalidResponseCompositeError, InvalidResponseError, InvalidSchemaNameError, InvalidUniqueIdError, LocationPreset, OAuthTokenSessionBundle, PremiumFeatureError, RoomGiftInfo, RoomInfo, SchemaDecodeError, ScreenPreset, SessionBundle, SignAPIError, SignatureMissingTokensError, SignatureRateLimitError, TikTokLiveConnectionBundledAuthOptions, TikTokLiveConnectionMutableOptions, TikTokLiveConnectionOptions, TikTokLiveConnectionProviderOptions, TikTokLiveConnectionState, TikTokLiveConstructorConnectionOptions, TikTokLiveError, UserOfflineError, WebSocketDynamicParams, WebSocketParams, WebcastEvent, WebcastEventMap, WebcastEventMessage, WebcastGotHttpConfig, WebcastHttpClientRequestParams, WebcastHttpCompositeRouteArgs, WebcastHttpEulerRouteArgs, WebcastHttpRouteArgs, WebcastMessage, WebcastTypedClient, WebcastTypedWebSocket, WebcastWebSocketEventMap, WrappedHttpRoute };
}
import * as import_tiktok_live_proto_v2 from "tiktok-live-proto/v2";
//#endregion
//#region src/lib/web/lib/cookie-jar.d.ts
/**
 * Custom cookie jar for got
 *
 * Exposes `beforeRequest` / `afterResponse` hooks that the HTTP client wires into `got.extend(...)`,
 * mirroring the axios interceptor model from before the migration.
 *
 * https://github.com/zerodytrash/TikTok-Livestream-Chat-Connector/issues/18
 */
declare class WebcastCookieJar implements AbstractWebcastCookieJar {
  readonly webConfig: WebcastWebConfigDefaults;
  /**
   * The internal cookie store, a simple key-value object. The keys are cookie names, the values are cookie values.
   */
  readonly store: Record<string, string>;
  /**
   * Constructor
   *
   * @param webConfig The current web config for the HTTP client
   */
  constructor(webConfig: WebcastWebConfigDefaults);
  /**
   * Set the session ID and tt-target-idc
   *
   * @param session The session bundle containing the session ID and tt-target-idc
   */
  setSessionBundle(session: CookieSessionBundle): Promise<void>;
  /**
   * Get the session bundle containing the session ID and tt-target-idc
   *
   */
  getSessionBundle(): Promise<CookieSessionBundle | null>;
  /**
   * Process a single set-cookie header
   *
   * @param setCookieHeader The set-cookie header
   */
  processSetCookieHeader(setCookieHeader: unknown): Promise<void>;
  /**
   * We ignore the URL parameter because TikTok's cookies are not scoped by path or domain - any cookie we set should be sent with every request to TikTok regardless of URL
   *
   * @param __url Ignored
   */
  getCookieString(__url?: string): Promise<string>;
  /**
   * Set cookie string - we ignore the URL parameter for the same reason as in getCookieString
   * @param rawCookie The raw cookie string from the set-cookie header
   * @param __url The URL the cookie is associated with (ignored)
   */
  setCookie(rawCookie: string, __url?: string): Promise<void>;
  /**
   * Serialize a cookie object into a cookie header string. For example, { sessionid: 'abc', tt-target-idc: 'def' } becomes 'sessionid=abc; tt-target-idc=def'
   *
   * @param cookies The cookie object to serialize
   */
  private static serializeCookieObject;
  /**
   * Serialize a CookieSessionBundle into a Record of cookie name to cookie value, based on the provided web config. This is used to set the initial cookies in the cookie jar when creating a new session.
   *
   * @param config The web config containing the cookie names to use for the session ID and tt-target-idc
   * @param session The session bundle containing the session ID and tt-target-idc to serialize
   *
   */
  static serializeCookieSessionBundle(config: WebcastWebConfigDefaults, session: CookieSessionBundle): string;
  /**
   * Parse a cookie header string into a Record of cookie name to cookie value. For example, 'sessionid=abc; tt-target-idc=def' becomes { sessionid: 'abc', tt-target-idc: 'def' }
   *
   * @param str
   */
  private static parseCookies;
}
//#endregion
//#region src/lib/web/lib/http-client.d.ts
declare class WebcastHttpClient {
  oAuthSessionBundle: OAuthTokenSessionBundle | null;
  protected readonly _gotInstance: Got;
  protected readonly _eulerApiInstance: EulerStreamApiClient;
  protected readonly _webcastWebConfig: WebcastWebConfigDefaults;
  readonly clientParams: Record<string, string>;
  readonly clientHeaders: Record<string, string>;
  readonly cookieJar: WebcastCookieJar;
  /**
   * Instantiate a Webcast HTTP Client
   *
   * @param webcastWebConfig Randomized config for Webcast requests
   * @param eulerApiInstance Rarely needed override for the cached & lazy-loaded API client
   * @param gotHttpConfig Rarely needed override for the Got HTTP library configuration
   */
  constructor(webcastWebConfig: WebcastWebConfigDefaults, eulerApiInstance?: EulerStreamApiClient, gotHttpConfig?: WebcastGotHttpConfig);
  /**
   * Get an instance of the underlying API Client
   */
  get apiClient(): EulerStreamApiClient;
  /**
   * Set the Room ID for the client
   *
   * @param roomId The client's Room ID
   */
  set roomId(roomId: string);
  /**
   * Get the Room ID for the client
   */
  get roomId(): string;
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
  request({
    host,
    path,
    searchParams,
    signRequest,
    method,
    headers,
    ...extraOptions
  }: WebcastHttpClientRequestParams): Promise<_$got.Response<string>>;
  /**
   * Get HTML from TikTok website
   *
   * @param path Path to the HTML page
   * @param options Additional request options
   */
  getHtmlFromTikTokWebsite(path: string, options?: Partial<WebcastHttpClientRequestParams>): Promise<string>;
  /**
   * Get deserialized object from Webcast API
   *
   * @param path Path to the API endpoint
   * @param params Query parameters to be sent with the request
   * @param schemaName Schema name for deserialization
   * @param signRequest Whether to sign the request or not
   * @param options Additional request options
   */
  getDeserializedObjectFromWebcastApi<T extends keyof WebcastMessage>(path: string, params: Record<string, string>, schemaName: T, signRequest?: boolean, options?: Partial<WebcastHttpClientRequestParams>): Promise<WebcastMessage[T]>;
  postJsonObjectToWebcastApi<T extends Record<string, any>>(path: string, params: Record<string, string>, data: Record<string, any>, signRequest?: boolean, options?: Partial<WebcastHttpClientRequestParams>): Promise<T>;
  /**
   * Get JSON object from Webcast API
   *
   * @param path Path to the API endpoint
   * @param params Query parameters to be sent with the request
   * @param signRequest Whether to sign the request or not
   * @param options Additional request options
   */
  getJsonObjectFromWebcastApi<T extends Record<string, any>>(path: string, params: Record<string, string>, signRequest?: boolean, options?: Partial<WebcastHttpClientRequestParams>): Promise<T>;
  /**
   * Get JSON object from TikTok API
   *
   * @param path Path to the API endpoint
   * @param params Query parameters to be sent with the request
   * @param signRequest Whether to sign the request or not
   * @param options Additional request options
   */
  getJsonObjectFromTikTokApi<T extends Record<string, any>>(path: string, params: Record<string, string>, signRequest?: boolean, options?: Partial<WebcastHttpClientRequestParams>): Promise<T>;
  /**
   * Determine if a host is secure
   *
   * @param host The host to check
   * @protected
   */
  protected isSecure(host: string): boolean;
  /**
   * Get the base URL from a host, no trailing slash
   *
   * @param host The host to get a base URL for
   * @protected
   */
  protected getBaseUrl(host: string): string;
}
//#endregion
//#region src/lib/web/routes/euler/config.d.ts
/**
 * Should never be changed, used to identify the library in requests to TikTok's APIs
 */
declare const LIBRARY_IDENTITY = "ttlive-node";
/**
 * Allow caching of the API client
 */
type SignConfig = Partial<ClientConfiguration> & {
  cachedInstance?: EulerStreamApiClient;
};
declare const SignConfig: SignConfig;
/**
 * Creates (or retrieves from cache) an instance of the Euler Stream API client configured with SignConfig.
 *
 * Should be a global singleton to avoid unnecessary re-instantiations and to allow for dynamic config changes (e.g. API key rotation).
 *
 */
declare function createEulerClient(): EulerStreamApiClient;
//#endregion
//#region src/lib/web/routes/euler/fetch-room-id-euler.d.ts
type FetchRoomIdFromEulerRouteParams = WebcastHttpEulerRouteArgs<{
  uniqueId: string;
}>;
/**
 * Fetches a room ID from Euler Stream using the provided uniqueId.
 *
 * @param apiClient The Euler Stream API client to use for the request.
 * @param uniqueId  The unique identifier (username) of the TikTok user whose room ID is being fetched.
 */
declare const fetchRoomIdFromEulerRoute: HttpRoute<FetchRoomIdFromEulerRouteParams, WebcastRoomIdRouteResponse>;
//#endregion
//#region src/lib/web/routes/euler/fetch-room-info-euler.d.ts
type FetchRoomInfoFromEulerRouteParams = WebcastHttpEulerRouteArgs<{
  uniqueId: string;
}>;
/**
 * Fetches full room information (streamer info, stats, status) from Euler Stream's premium endpoint.
 *
 * @param apiClient The Euler Stream API client to use for the request.
 * @param uniqueId  The unique identifier (username) of the TikTok user whose room info is being fetched.
 */
declare const fetchRoomInfoFromEulerRoute: HttpRoute<FetchRoomInfoFromEulerRouteParams, WebcastRoomInfoRouteResponse>;
//#endregion
//#region src/lib/web/routes/euler/fetch-signed-websocket-euler.d.ts
type FetchSignedWebSocketFromEulerRouteParams = WebcastHttpEulerRouteArgs<TikTokLiveConnectionBundledAuthOptions & {
  roomId: string;
  country?: SoaxProxyRegion;
  cursor?: string;
}>;
type FetchSignedWebSocketFromEulerRouteResponse = {
  fetchResult: index_d_exports.ProtoMessageFetchResult;
  fetchResultCookieHeader: string;
  fetchResultRoomId?: string;
};
/**
 * Fetches a signed TikTok WebSocket URL from Euler Stream along with the initial `ProtoMessageFetchResult`
 * and any cookies the sign server set.
 *
 * Enforces whitelist validation (via `WHITELIST_AUTHENTICATED_SESSION_ID_HOST`) when authenticated WebSocket
 * connections are requested. Translates sign-server HTTP responses into typed errors
 * (`SignAPIError`, `SignatureRateLimitError`, `PremiumFeatureError`).
 */
declare const fetchSignedWebSocketFromEulerRoute: HttpRoute<FetchSignedWebSocketFromEulerRouteParams, FetchSignedWebSocketFromEulerRouteResponse>;
//#endregion
//#region src/lib/web/routes/euler/fetch-webcast-signature-euler.d.ts
type WebcastSignerParams = {
  url: string | URL;
  method: SignTikTokUrlBodyMethodEnum;
  userAgent: string;
};
type WebcastSignerResponse = SignWebcastUrl200Response;
type FetchWebcastSignatureFromEulerRouteParams = WebcastHttpEulerRouteArgs<WebcastSignerParams>;
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
declare const fetchWebcastSignatureFromEulerRoute: HttpRoute<FetchWebcastSignatureFromEulerRouteParams, SignWebcastUrl200Response>;
//#endregion
//#region src/lib/web/routes/euler/send-room-chat-euler.d.ts
type SendRoomChatFromEulerRouteParams = WebcastHttpEulerRouteArgs<{
  roomId: string;
  content: string;
}>;
/**
 * Sends a chat message into a TikTok LIVE room via Euler Stream's premium endpoint. Supports either
 * cookie-based (`sessionId` + `ttTargetIdc`) or OAuth-token session bundles; OAuth takes precedence when present.
 *
 * Throws `PremiumFeatureError` when the sign server returns 401/403, and `InvalidResponseError` for
 * any other non-200 response.
 */
declare const sendRoomChatFromEulerRoute: HttpRoute<SendRoomChatFromEulerRouteParams, WebcastRoomChatRouteResponse>;
//#endregion
//#region src/lib/web/routes/base/fetch-room-gifts.d.ts
type RoomGiftsRouteParams = WebcastHttpRouteArgs<{
  roomId?: string;
}>;
type RoomGiftsResponse = any;
/**
 * Fetches the list of gifts available in a TikTok LIVE room. If `roomId` is not provided, the
 * `roomId` currently set on the `webClient` is used.
 *
 * @param webClient The HTTP client instance to use for the request.
 * @param roomId The ID of the room whose gift list to fetch. Optional if the webClient has a roomId context.
 */
declare const fetchRoomGiftsRoute: HttpRoute<RoomGiftsRouteParams, any>;
//#endregion
//#region src/lib/web/routes/base/fetch-room-info.d.ts
type RoomInfoRouteParams = WebcastHttpRouteArgs<{
  roomId?: string;
}>;
type RoomInfoResponse = any;
/**
 * Fetches room information for a given roomId. If roomId is not provided, it will attempt to use the roomId from the webClient context.
 *
 * @param webClient The HTTP client instance to use for the request.
 * @param roomId The ID of the room to fetch information for. Optional if the webClient has a roomId context.
 */
declare const fetchRoomInfoRoute: HttpRoute<RoomInfoRouteParams, any>;
//#endregion
//#region src/lib/web/routes/base/fetch-room-info-api-live.d.ts
type FetchRoomInfoFromApiRouteParams = WebcastHttpRouteArgs<{
  uniqueId: string;
}>;
type FetchRoomInfoFromApiRouteResponse = {
  statusCode: number;
  message?: string;
  data?: {
    user?: Record<string, any> & {
      roomId: string;
    };
    liveRoom?: Record<string, any> & {
      status: number;
      roomId: string;
    };
  };
};
/**
 * Fetches room information from the TikTok API Live endpoint using the provided uniqueId. This route is used as part of the process to determine if a user is currently live streaming and to retrieve associated room information.
 *
 * @param webClient The HTTP client instance to use for making the API request.
 * @param uniqueId The unique identifier (username) of the TikTok user whose room information is being fetched.
 */
declare const fetchRoomInfoFromApiLiveRoute: HttpRoute<FetchRoomInfoFromApiRouteParams, FetchRoomInfoFromApiRouteResponse>;
//#endregion
//#region src/lib/web/routes/base/fetch-room-info-html.d.ts
/**
 * Mutable configuration for `fetchRoomInfoFromHtmlRoute`. Override `extractionPattern`
 * if TikTok ever changes the markup wrapping the SIGI_STATE JSON blob.
 */
declare const RoomInfoFromHtmlRouteConfig: {
  extractionPattern: RegExp;
};
type FetchRoomInfoFromHtmlRouteParams = WebcastHttpRouteArgs<{
  uniqueId: string;
}>;
type FetchRoomInfoFromHtmlRouteResponse = Record<string, any> & {
  liveRoomUserInfo?: FetchRoomInfoFromApiRouteResponse['data'];
};
/**
 * Fetches room information by scraping the HTML content of the TikTok user's live page.
 * This route is used as a fallback method to retrieve room information when API endpoints are unavailable or blocked. It extracts the SIGI_STATE JSON data embedded in the HTML, which contains various details about the live room and user information.
 *
 * @param webClient The HTTP client instance to use for making the request to the TikTok website.
 * @param uniqueId The unique identifier (username) of the TikTok user whose room information is being fetched.
 */
declare const fetchRoomInfoFromHtmlRoute: HttpRoute<FetchRoomInfoFromHtmlRouteParams, FetchRoomInfoFromHtmlRouteResponse>;
//#endregion
//#region src/lib/web/routes/composite/fetch-room-id.d.ts
type FetchRoomIdRouteParams = WebcastHttpCompositeRouteArgs<{
  uniqueId: string;
}>;
declare const RoomIdRouteConfig: {
  skipFetchRoomInfoFromHtmlRoute: boolean;
  skipFetchRoomInfoFromApiLiveRoute: boolean;
  skipFetchRoomIdFromEulerRoute: boolean;
};
/**
 * Resolves a room ID across multiple sources, falling back progressively:
 *   1. HTML scrape (SIGI_STATE)
 *   2. TikTok API Live endpoint
 *   3. Euler Stream (skipped if `RoomIdRouteConfig.skipFetchRoomIdFromEulerRoute` is true)
 *
 * Errors from each source are accumulated and surfaced via `InvalidResponseCompositeError` if every source fails.
 */
declare const fetchRoomIdComposite: HttpRoute<FetchRoomIdRouteParams, string>;
//#endregion
//#region src/lib/web/routes/composite/fetch-is-live.d.ts
type FetchIsLiveRouteParams = WebcastHttpCompositeRouteArgs<{
  uniqueId: string;
}>;
declare const IsLiveRouteConfig: {
  skipFetchRoomInfoFromHtmlRoute: boolean;
  skipFetchRoomInfoFromApiLiveRoute: boolean;
  skipFetchRoomIdFromEulerRoute: boolean;
};
/**
 * Determines whether a user is currently live, falling back progressively:
 *   1. HTML scrape (SIGI_STATE status field)
 *   2. TikTok API Live endpoint (liveRoom.status)
 *   3. Euler Stream (is_live flag — skipped if `disableEulerFallback` is true)
 *
 * Errors from each source are accumulated and surfaced via `FetchIsLiveError` if every source fails.
 */
declare const fetchIsLiveComposite: HttpRoute<FetchIsLiveRouteParams, boolean>;
//#endregion
//#region src/lib/web/routes/routes.d.ts
declare enum BaseFetchRoute {
  FETCH_ROOM_GIFTS = "fetchRoomGiftsRoute",
  FETCH_ROOM_INFO = "fetchRoomInfoRoute",
  FETCH_ROOM_INFO_API_LIVE = "fetchRoomInfoApiLiveRoute",
  FETCH_ROOM_INFO_HTML = "fetchRoomInfoHtmlRoute"
}
declare enum CompositeFetchRoute {
  FETCH_IS_LIVE = "fetchIsLiveRoute",
  FETCH_ROOM_ID = "fetchRoomIdRoute"
}
declare enum EulerFetchRoute {
  FETCH_ROOM_ID = "fetchRoomIdFromEulerRoute",
  FETCH_ROOM_INFO = "fetchRoomInfoFromEulerRoute",
  FETCH_SIGNED_WEBSOCKET = "fetchSignedWebSocketFromEulerRoute",
  FETCH_WEBCAST_SIGNATURE = "fetchWebcastSignatureFromEulerRoute",
  SEND_ROOM_CHAT = "sendRoomChatFromEulerRoute"
}
//#endregion
//#region src/lib/client/utilities.d.ts
declare function HandleError(errMsg: string, exceptionWrapper?: new (err: Error, ...args: any[]) => Error): <This, Args extends any[], Return>(originalMethod: (this: This, ...args: Args) => Return, _context: ClassMethodDecoratorContext<This, (this: This, ...args: Args) => Return>) => (this: This, ...args: Args) => Return;
//#endregion
//#region src/lib/client/index.d.ts
declare const TikTokLiveConnection_base: WebcastTypedClient;
declare class TikTokLiveConnection extends TikTokLiveConnection_base {
  readonly uniqueId: string;
  readonly options: TikTokLiveConnectionMutableOptions;
  protected _webClient: WebcastHttpClient;
  protected _wsClientInstance: WebcastWebSocketClient | null;
  protected _wsClientProvider: (params: WebSocketDynamicParams) => WebcastWebSocketClient;
  protected _devicePresets: GetWebConfigParams;
  protected _roomInfo: RoomInfo | null;
  protected _availableGifts: RoomGiftsResponse | null;
  protected _connectState: ConnectState;
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
  constructor(uniqueId: string, options: TikTokLiveConstructorConnectionOptions);
  /**
   * Get the current Web Client instance
   */
  get webClient(): WebcastHttpClient;
  /**
   * Get an instance of the underlying API Client
   */
  get apiClient(): EulerStreamApiClient;
  /**
   * Get the current WebSocket client instance. Will be `null` if not connected.
   */
  get wsClient(): WebcastWebSocketClient | null;
  /**
   * Set the connection state to disconnected
   * @protected
   */
  protected setDisconnected(): void;
  /**
   * Get the current Room Info
   */
  get roomInfo(): any;
  /**
   * Get the available gifts
   */
  get availableGifts(): any;
  /**
   * Get the current connection state
   */
  get isConnecting(): boolean;
  /**
   * Check if the connection is established
   */
  get isConnected(): boolean;
  /**
   * Get the current client parameters
   */
  get clientParams(): Record<string, string>;
  /**
   * Get the current room ID
   */
  get roomId(): string;
  /**
   * Get the current connection state including the cached room info and all available gifts
   * (if `enableExtendedGiftInfo` option enabled)
   */
  get state(): TikTokLiveConnectionState;
  /**
   * Connects to the live stream of the specified streamer
   *
   * @param roomId Room ID to connect to. If not specified, the room ID will be retrieved from the TikTok API
   * @returns The current connection state
   */
  connect(roomId?: string): Promise<TikTokLiveConnectionState>;
  /**
   * Connects to the live stream of the specified streamer
   *
   * @param roomId Room ID to connect to. If not specified, the room ID will be retrieved from the TikTok API
   * @protected
   */
  protected _connect(roomId?: string): Promise<void>;
  /**
   * Disconnects the connection to the live stream
   */
  disconnect(): Promise<void>;
  /**
   * Fetch the room ID from the TikTok API
   *
   * @param uniqueId Optional unique ID to use instead of the current one
   */
  fetchRoomId(uniqueId?: string): Promise<string>;
  /**
   * Fetch whether the streamer is currently live
   */
  fetchIsLive(uniqueId?: string): Promise<boolean>;
  /**
   * Get the current room info (including streamer info, room status and statistics)
   * @returns Promise that will be resolved when the room info has been retrieved from the API
   */
  fetchRoomInfo(roomId?: string): Promise<RoomInfoResponse>;
  /**
   * Get the available gifts in the current room
   * @returns Promise that will be resolved when the available gifts have been retrieved from the API
   */
  fetchAvailableGifts(): Promise<RoomGiftInfo>;
  /**
   * Send a message to a TikTok LIVE Room
   *
   * @param content Message content to send to the stream
   * @param roomId Target room ID. If not specified, the message will be sent to the currently connected room. Note that a room ID is required to send a message, so if you're not currently connected to a room you must specify a room ID.
   */
  sendMessage(content: string, roomId?: string): Promise<WebcastRoomChatRouteResponse>;
  /**
   * Set up the WebSocket connection
   *
   * @param wsUrl WebSocket URL
   * @param wsParams WebSocket parameters
   * @param roomId The room ID to connect to with the WebSocket client
   * @returns Promise that will be resolved when the WebSocket connection is established
   * @protected
   */
  protected setupWebsocket(wsUrl: string, wsParams: WebSocketParams, roomId: string): Promise<void>;
  protected processProtoMessageFetchResult(protoMessageFetchResult: ProtoMessageFetchResult): Promise<void>;
  protected processDecodedData({
    data,
    type
  }: DecodedData): Promise<boolean | void>;
  /**
   * Handle the error event
   *
   * @param exception Exception object
   * @param info Additional information about the error
   * @protected
   */
  protected handleError(exception: Error, info: string): void;
  /**
   * Wait until the streamer is live
   *
   * @param seconds Number of seconds to wait before checking if the streamer is live again
   * @param abortSignal Process will keep checking if the streamer is live every `seconds` seconds until the streamer is live or the abort signal is triggered
   *
   */
  waitUntilLive(seconds?: number, abortSignal?: AbortSignal): Promise<void>;
}
//#endregion
export { InvalidRequestError as $, GetWebConfigParams as $t, fetchWebcastSignatureFromEulerRoute as A, SessionBundle as At, createEulerClient as B, WebcastWebConfigDefaults as Bt, RoomGiftsRouteParams as C, CookieSessionBundle as Ct, FetchWebcastSignatureFromEulerRouteParams as D, OAuthTokenSessionBundle as Dt, sendRoomChatFromEulerRoute as E, IWebcastDeserializeConfig as Et, fetchRoomInfoFromEulerRoute as F, TikTokLiveConstructorConnectionOptions as Ft, WebcastHttpEulerRouteArgs as G, deserializeWebSocketMessage as Gt, EulerRouteRequestOptions as H, WebcastDeserializeConfig as Ht, FetchRoomIdFromEulerRouteParams as I, WebSocketParams as It, AlreadyConnectedError as J, WebSocketConfigDefaults as Jt, WebcastHttpRouteArgs as K, validateAndNormalizeUniqueId as Kt, fetchRoomIdFromEulerRoute as L, WebcastEventMessage as Lt, FetchSignedWebSocketFromEulerRouteResponse as M, TikTokLiveConnectionMutableOptions as Mt, fetchSignedWebSocketFromEulerRoute as N, TikTokLiveConnectionOptions as Nt, WebcastSignerParams as O, RoomGiftInfo as Ot, FetchRoomInfoFromEulerRouteParams as P, TikTokLiveConnectionProviderOptions as Pt, ErrorReason as Q, DevicePreset as Qt, LIBRARY_IDENTITY as R, WebcastHttpClientRequestParams as Rt, RoomGiftsResponse as S, WebcastWebSocketEventMap as St, SendRoomChatFromEulerRouteParams as T, DecodedWebcastPushFrame as Tt, HttpRoute as U, createBaseWebcastPushFrame as Ut, index_d_exports as V, generateUniqId as Vt, WebcastHttpCompositeRouteArgs as W, deserializeMessage as Wt, AuthenticatedWebSocketConnectionError as X, WebcastWebSocketConfigDefaults as Xt, AlreadyConnectingError as Y, WebcastWebSocketConfig as Yt, ConnectTimeoutError as Z, AbstractWebcastCookieJar as Zt, FetchRoomInfoFromApiRouteResponse as _, WebcastEvent as _t, EulerFetchRoute as a, SchemaDecodeError as at, RoomInfoRouteParams as b, createWebSocketProvider as bt, fetchIsLiveComposite as c, SignatureRateLimitError as ct, fetchRoomIdComposite as d, BasicWebcastEventMessage as dt, GetWebSocketConfigParams as en, InvalidResponseCompositeError as et, FetchRoomInfoFromHtmlRouteParams as f, ClientEventMap as ft, FetchRoomInfoFromApiRouteParams as g, TikTokLiveConnectionState as gt, fetchRoomInfoFromHtmlRoute as h, EventHandler as ht, CompositeFetchRoute as i, WebcastGotHttpConfig as in, PremiumFeatureError as it, FetchSignedWebSocketFromEulerRouteParams as j, TikTokLiveConnectionBundledAuthOptions as jt, WebcastSignerResponse as k, RoomInfo as kt, FetchRoomIdRouteParams as l, TikTokLiveError as lt, RoomInfoFromHtmlRouteConfig as m, ControlEvent as mt, HandleError as n, ScreenPreset as nn, InvalidSchemaNameError as nt, FetchIsLiveRouteParams as o, SignAPIError as ot, FetchRoomInfoFromHtmlRouteResponse as p, ConnectState as pt, WrappedHttpRoute as q, getWebSocketConfigDefaults as qt, BaseFetchRoute as r, WebSocketDynamicParams as rn, InvalidUniqueIdError as rt, IsLiveRouteConfig as s, SignatureMissingTokensError as st, TikTokLiveConnection as t, LocationPreset as tn, InvalidResponseError as tt, RoomIdRouteConfig as u, UserOfflineError as ut, fetchRoomInfoFromApiLiveRoute as v, WebcastEventMap as vt, fetchRoomGiftsRoute as w, DecodedData as wt, fetchRoomInfoRoute as x, WebcastTypedWebSocket as xt, RoomInfoResponse as y, WebcastTypedClient as yt, SignConfig as z, WebcastMessage as zt };
//# sourceMappingURL=index-DYHjCe5b.d.ts.map
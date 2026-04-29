import { Ft as TikTokLiveConstructorConnectionOptions, Lt as WebcastEventMessage, t as TikTokLiveConnection } from "./index-DYHjCe5b.js";
import { EventEmitter } from "node:events";
import { ProtoMessageFetchResult } from "tiktok-live-proto/v2";

//#region src/lib/client/legacy/data-converter.d.ts
/**
 * This ugly function brings the nested protobuf objects to a flat level
 * In addition, attributes in "Long" format are converted to strings (e.g. UserIds)
 * This makes it easier to handle the data later, since some libraries have problems to serialize this protobuf specific data.
 */
declare function simplifyObject(type: keyof WebcastEventMessage, originalObject: any): WebcastEventMessage & Record<string, any>;
declare function getEventAttributes(event: any): any;
declare function getTopViewerAttributes(topViewers: any): any;
declare function mapBadges(badges: any): never[];
declare function getPreferredPictureFormat(pictureUrls: any): any;
//#endregion
//#region src/lib/client/legacy/index.d.ts
/**
 * Cast that preserves the real `(uniqueId, options)` constructor signature while exposing the
 * loose `EventEmitter.emit(...)` overload. Required because the legacy class emits the flattened
 * `simplifiedObj` shape, which doesn't satisfy the strict `ClientEventMap` payload types.
 */
type WebcastPushConnectionBase = new (uniqueId: string, options: TikTokLiveConstructorConnectionOptions) => EventEmitter & TikTokLiveConnection;
declare const WebcastPushConnection_base: WebcastPushConnectionBase;
/**
 * The legacy WebcastPushConnection class for backwards compatibility.
 * @deprecated Use TikTokLiveConnection instead.
 */
declare class WebcastPushConnection extends WebcastPushConnection_base {
  protected processProtoMessageFetchResult(fetchResult: ProtoMessageFetchResult): Promise<void>;
}
//#endregion
export { WebcastPushConnection, getEventAttributes, getPreferredPictureFormat, getTopViewerAttributes, mapBadges, simplifyObject };
//# sourceMappingURL=legacy.d.ts.map
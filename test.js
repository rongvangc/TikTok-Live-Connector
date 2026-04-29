import { ControlEvent, deserializeMessage, TikTokLiveConnection, WebcastEvent } from './dist/index.js';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { launchPersistentContext } from 'cloakbrowser';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const username = 'mplph_official';
const sessionId = '';
const ttTargetIdc = 'alisg';

const connection = new TikTokLiveConnection(username, {
    sessionId,
    ttTargetIdc,
    authenticateWs: true,
    signedWebSocketProvider: async () => {
        const browser = await launchPersistentContext({
            userDataDir: path.resolve(__dirname, 'profiles', sessionId),
        });
        const page = await browser.newPage();

        try {
            await page.context().addCookies([
                {
                    name: 'sessionid',
                    value: sessionId,
                    domain: '.tiktok.com',
                    path: '/',
                },
                {
                    name: 'sessionid_ss',
                    value: sessionId,
                    domain: '.tiktok.com',
                    path: '/',
                },
            ]);

            await page.goto(`https://www.tiktok.com/@${username}/live`);

            return await new Promise((resolve, reject) => {
                const timeoutId = setTimeout(() => reject(), 120_000);

                page.on('response', async (response) => {
                    try {
                        const url = response.url();
                        if (!url.startsWith('https://webcast.tiktok.com/webcast/im/fetch/')) return;

                        const status = response.status();
                        if (status !== 200) return;

                        const responseHeaders = await response.headersArray();
                        for (const header of responseHeaders) {
                            if (header.name.toLowerCase() === 'set-cookie') {
                                connection.webClient.cookieJar.processSetCookieHeader(header.value);
                            }
                        }

                        const responseBody = await response.body();
                        resolve(deserializeMessage('ProtoMessageFetchResult', responseBody));
                    } catch (error) {
                        reject(error);
                    } finally {
                        clearTimeout(timeoutId);
                    }
                });
            });
        } catch (error) {
            console.error(error);
        } finally {
            await browser.close();
        }
    },
});

const isLive = await connection.fetchIsLive();
if (!isLive) {
    console.info('The streamer is not live.');
    process.exit(0);
}

connection
    .connect()
    .then((state) => {
        console.info('Connected successfully', state.roomInfo.data.status);
    })
    .catch((err) => {
        console.error('Failed to connect', err);
    });

connection.on(ControlEvent.WEBSOCKET_CONNECTED, (client) => {
    console.log('WebSocket open:', client.open);
});

connection.on(WebcastEvent.CHAT, (data) => {
    console.log(`${data.user.uniqueId}: ${data.comment}`);
});

connection.on(WebcastEvent.FOLLOW, (data) => {
    console.log(`${data.user.uniqueId} followed the streamer!`);
});

connection.on(WebcastEvent.LIKE, (data) => {
    console.log(`${data.user.uniqueId} liked the stream!`);
});

connection.on(WebcastEvent.MEMBER, (data) => {
    const uniqueId = data.user.uniqueId;
    const nickname = data.user.nickname;

    if (uniqueId) {
        console.log(`User uniqueId: ${uniqueId}`);
    }

    if (nickname) {
        console.log(`User nickname: ${nickname}`);
    }

    if (uniqueId || nickname) {
        console.log('User joined the stream!');
    }

    console.log(`Viewers: ${data.memberCount}`);
});

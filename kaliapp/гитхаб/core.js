(function() {
    'use strict';

    const CONFIG = {
        API_BASE: document.querySelector('meta[name="mirror:api-base"]')?.content || 'https://nekhebet.su',
        CHANNEL_ID: document.querySelector('meta[name="mirror:channel-id"]')?.content,
        CHANNEL_TITLE: document.querySelector('meta[name="mirror:channel-title"]')?.content,
        CHANNEL_USERNAME: document.querySelector('meta[name="mirror:channel-username"]')?.content,
        CHANNEL_AVATAR: document.querySelector('meta[name="mirror:channel-avatar"]')?.content || 'avatar.jpg',
        INITIAL_LIMIT: 25,
        MAX_RECONNECT_ATTEMPTS: 10,
        RECONNECT_BASE_DELAY: 1000,
        MAX_VISIBLE_POSTS: 100,
        LAZY_LOAD_OFFSET: 500,
        IMAGE_UNLOAD_DISTANCE: 5000,
        DEDUP_TTL: 200,
        WS_BASE: (() => {
            const apiBase = document.querySelector('meta[name="mirror:api-base"]')?.content;
            return apiBase.replace('http://', 'ws://').replace('https://', 'wss://');
        })(),
        SYNC_AFTER_RECONNECT: true,
        PING_INTERVAL: 30000,
        API_VERSION: 'v1',
        RECONNECT_GIVE_UP_DELAY: 300000,
        MAX_CACHE_SIZE: 200,
        MAX_MEDIA_CACHE_SIZE: 100,
        CLEANUP_INTERVAL: 2592000,
        MEDIA_MAX_RETRIES: 5,
        MEDIA_RETRY_DELAY: 2000,
        SUPPORTS_MEDIA_STATUS: true,
        MEDIA_STATUS_POLL_INTERVAL: 5000,
        MEDIA_PENDING_TIMEOUT: 300000,
        MEDIA_FAILED_TIMEOUT: 3600000,
        MEDIA_READY_DELAY: 1500,
        MEDIA_RETRY_ON_ERROR: true,
        MAX_CONCURRENT_LOADS: 5,
        MEDIA_LOAD_TIMEOUT: 5000,
        VIDEO_PRELOAD: 'metadata',
        RETRY_ON_NETWORK_ERROR: true,
        USE_LAZY_LOADING: true
    };

    const State = {
        posts: new Map(),
        postOrder: [],
        newPosts: [],
        offset: 0,
        hasMore: true,
        isLoading: false,
        ws: null,
        wsConnected: false,
        wsReconnectAttempts: 0,
        mediaCache: new Map(),
        mediaErrorCache: new Set(),
        mediaLoading: new Set(),
        mediaPending: new Map(),
        mediaRetryCount: new Map(),
        mediaRetryTimeouts: new Map(),
        mediaStatusStore: new Map(),
        mediaReadyQueue: new Set(),
        activeLoads: 0,
        loadQueue: [],
        failedMedia: new Map(),
        scrollTimeout: null,
        recentMessages: new Map(),
        lastDocumentHeight: 0,
        theme: localStorage.getItem('theme') || (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'),
        visiblePosts: new Set(),
        isTransitioning: false,
        pendingTheme: null,
        domCache: null,
        scrollPosition: 0,
        intervals: [],
        timeouts: [],
        resizeObserver: null,
        wsPingInterval: null,
        fullMessageCache: new Map(),
        loadingMessages: new Set(),
        initialLoadComplete: false,
        pendingEvents: [],
        lastEventId: 0,
        currentlyPlayingVideo: null,
        batchProcessed: new Set(),
        pendingEdits: new Set(),
        pendingNews: new Set(),
        supportedVersions: ['2.0', '3.0'],
        serverVersion: '3.0'
    };

    function cleanupResources() {
        State.intervals.forEach(clearInterval);
        State.intervals = [];
        State.timeouts.forEach(clearTimeout);
        State.timeouts = [];
        if (State.resizeObserver) {
            State.resizeObserver.disconnect();
            State.resizeObserver = null;
        }
        if (State.wsPingInterval) {
            clearInterval(State.wsPingInterval);
            State.wsPingInterval = null;
        }
        State.mediaRetryTimeouts.forEach(clearTimeout);
        State.mediaRetryTimeouts.clear();
        State.loadQueue = [];
        State.activeLoads = 0;
    }

    function safeSetTimeout(fn, delay) {
        const id = setTimeout(() => {
            const index = State.timeouts.indexOf(id);
            if (index > -1) State.timeouts.splice(index, 1);
            fn();
        }, delay);
        State.timeouts.push(id);
        return id;
    }

    function safeSetInterval(fn, delay) {
        const id = setInterval(fn, delay);
        State.intervals.push(id);
        return id;
    }

    const CacheManager = {
        cleanup() {
            const now = Date.now();
            if (State.fullMessageCache.size > CONFIG.MAX_CACHE_SIZE) {
                const keysToDelete = Array.from(State.fullMessageCache.keys()).slice(0, State.fullMessageCache.size - CONFIG.MAX_CACHE_SIZE);
                keysToDelete.forEach(key => State.fullMessageCache.delete(key));
            }
            if (State.mediaCache.size > CONFIG.MAX_MEDIA_CACHE_SIZE) {
                const keysToDelete = Array.from(State.mediaCache.keys()).slice(0, State.mediaCache.size - CONFIG.MAX_MEDIA_CACHE_SIZE);
                keysToDelete.forEach(key => State.mediaCache.delete(key));
            }
            for (const [messageId, data] of State.mediaStatusStore.entries()) {
                if (data.status !== 'ready' && now - data.lastUpdate > 300000) {
                    State.mediaStatusStore.delete(messageId);
                }
                if (data.status === 'failed' && now - data.lastUpdate > CONFIG.MEDIA_FAILED_TIMEOUT) {
                    State.mediaStatusStore.delete(messageId);
                }
            }
            const ttl = CONFIG.DEDUP_TTL * 2;
            for (const [key, time] of State.recentMessages.entries()) {
                if (now - time > ttl) {
                    State.recentMessages.delete(key);
                }
            }
            const fiveMinutesAgo = now - 300000;
            for (const [messageId, data] of State.mediaPending.entries()) {
                if (data.timestamp < fiveMinutesAgo) {
                    State.mediaPending.delete(messageId);
                    State.mediaRetryCount.delete(messageId);
                }
            }
            for (const [messageId, data] of State.failedMedia.entries()) {
                if (now - data.timestamp > 3600000) {
                    State.failedMedia.delete(messageId);
                }
            }
        },
        startCleanupInterval() {
            return safeSetInterval(() => this.cleanup(), CONFIG.CLEANUP_INTERVAL);
        }
    };

    const Security = {
        escapeHtml(unsafe) {
            const div = document.createElement('div');
            div.textContent = unsafe;
            return div.innerHTML;
        },
        sanitizeUrl(url) {
            try {
                const parsed = new URL(url);
                if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') return '#';
                if (parsed.hostname === 'localhost' || parsed.hostname === '127.0.0.1') return '#';
                return url;
            } catch {
                return '#';
            }
        },
        validateMessageId(id) {
            return Number.isInteger(Number(id)) && Number(id) > 0;
        }
    };

    const Formatters = {
        formatDate(date) {
            if (!date) return '';
            let d = new Date(date);
            if (isNaN(d.getTime())) {
                d = new Date(date + 'Z');
            }
            if (isNaN(d.getTime())) return 'Invalid date';
            d.setHours(d.getHours() + 2);
            const now = new Date();
            const isToday = d.toDateString() === now.toDateString();
            const isYesterday = new Date(now.setDate(now.getDate() - 1)).toDateString() === d.toDateString();
            const time = d.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
            if (isToday) return `Сегодня в ${time}`;
            if (isYesterday) return `Вчера в ${time}`;
            const dateStr = d.toLocaleDateString('ru-RU', { day: '2-digit', month: 'long', year: d.getFullYear() !== now.getFullYear() ? 'numeric' : undefined });
            return `${dateStr} в ${time}`;
        },
        formatViews(views) {
            if (!views) return '0';
            if (views >= 1000000) return `${(views / 1000000).toFixed(1)}M`;
            if (views >= 1000) return `${(views / 1000).toFixed(1)}K`;
            return views.toString();
        },
        formatText(text, entities = []) {
            if (!text) return '';
            const isEmoji = (char) => {
                const codePoint = char.codePointAt(0);
                return (codePoint >= 0x1F300 && codePoint <= 0x1F9FF) ||
                       (codePoint >= 0x2600 && codePoint <= 0x26FF) ||
                       (codePoint >= 0x2700 && codePoint <= 0x27BF) ||
                       (codePoint >= 0x1F1E6 && codePoint <= 0x1F1FF) ||
                       codePoint === 0x200D ||
                       (codePoint >= 0xE0020 && codePoint <= 0xE007F);
            };
            const emojiSequences = [];
            let processed = '';
            for (let i = 0; i < text.length; i++) {
                const char = text[i];
                if (isEmoji(char)) {
                    let sequence = '';
                    while (i < text.length && isEmoji(text[i])) {
                        sequence += text[i];
                        i++;
                    }
                    i--;
                    const placeholder = `%%%EMOJI${emojiSequences.length}%%%`;
                    emojiSequences.push(sequence);
                    processed += placeholder;
                } else {
                    processed += char;
                }
            }
            const codeBlocks = [];
            let processedWithCode = processed;
            processedWithCode = processedWithCode.replace(/```([\s\S]*?)```/g, (match, code) => {
                const placeholder = `%%%CODEBLOCK${codeBlocks.length}%%%`;
                codeBlocks.push({ type: 'pre', content: code });
                return placeholder;
            });
            processedWithCode = processedWithCode.replace(/`([^`]+)`/g, (match, code) => {
                const placeholder = `%%%CODEBLOCK${codeBlocks.length}%%%`;
                codeBlocks.push({ type: 'inline', content: code });
                return placeholder;
            });
            let escaped = Security.escapeHtml(processedWithCode);
            escaped = escaped.replace(/%%%EMOJI(\d+)%%%/g, (match, index) => {
                return emojiSequences[parseInt(index)] || match;
            });
            const linkPlaceholders = [];
            escaped = escaped.replace(/\[([^\]]+)\]\(([^)]+?)(?:\s+"[^"]*")?\)/g, (match, linkText, url) => {
                url = url.replace(/[<>"']/g, '');
                const safeUrl = Security.sanitizeUrl(url);
                if (safeUrl === '#') return match;
                const placeholder = `%%%LINK${linkPlaceholders.length}%%%`;
                linkPlaceholders.push({ type: 'markdown', content: linkText, url: safeUrl });
                return placeholder;
            });
            const urlParts = [];
            let lastIndex = 0;
            const urlRegex = /(https?:\/\/[^\s<>"']+)/g;
            let match;
            while ((match = urlRegex.exec(escaped)) !== null) {
                if (match.index > lastIndex) {
                    urlParts.push(escaped.substring(lastIndex, match.index));
                }
                const url = match[0];
                const safeUrl = Security.sanitizeUrl(url);
                if (safeUrl === '#') {
                    urlParts.push(url);
                } else {
                    const placeholder = `%%%LINK${linkPlaceholders.length}%%%`;
                    linkPlaceholders.push({ type: 'auto', content: url, url: safeUrl });
                    urlParts.push(placeholder);
                }
                lastIndex = match.index + match[0].length;
            }
            if (lastIndex < escaped.length) {
                urlParts.push(escaped.substring(lastIndex));
            }
            escaped = urlParts.join('');
            if (entities && entities.length > 0) {
                const sortedEntities = [...entities].sort((a, b) => b.offset - a.offset);
                for (const entity of sortedEntities) {
                    const { offset, length, type } = entity;
                    if (offset < 0 || offset + length > escaped.length) continue;
                    const before = escaped.substring(0, offset);
                    const content = escaped.substring(offset, offset + length);
                    const after = escaped.substring(offset + length);
                    let wrapped = content;
                    switch (type) {
                        case 'bold':
                        case 'Bold':
                            wrapped = `<b>${content}</b>`;
                            break;
                        case 'italic':
                        case 'Italic':
                            wrapped = `<i>${content}</i>`;
                            break;
                        case 'underline':
                        case 'Underline':
                            wrapped = `<u>${content}</u>`;
                            break;
                        case 'strikethrough':
                        case 'Strikethrough':
                            wrapped = `<s>${content}</s>`;
                            break;
                        case 'code':
                        case 'pre':
                            wrapped = content;
                            break;
                        case 'spoiler':
                        case 'Spoiler':
                            wrapped = `<span class="tg-spoiler" onclick="this.classList.toggle('revealed')">${content}</span>`;
                            break;
                        case 'text_link':
                            if (entity.url) {
                                const safeUrl = Security.sanitizeUrl(entity.url);
                                const linkPlaceholder = `%%%LINK${linkPlaceholders.length}%%%`;
                                linkPlaceholders.push({ type: 'entity', content: content, url: safeUrl });
                                wrapped = linkPlaceholder;
                            }
                            break;
                        case 'mention':
                            wrapped = `<span class="tg-mention" data-mention="${content}">${content}</span>`;
                            break;
                        case 'hashtag':
                            wrapped = `<span class="tg-hashtag" data-hashtag="${content}">${content}</span>`;
                            break;
                    }
                    escaped = before + wrapped + after;
                }
            } else {
                let formatted = escaped;
                formatted = formatted.replace(/\*\*\*(.*?)\*\*\*/g, '<b><i>$1</i></b>');
                formatted = formatted.replace(/\*\*(.*?)\*\*/g, '<b>$1</b>');
                formatted = formatted.replace(/__(.*?)__/g, '<u>$1</u>');
                formatted = formatted.replace(/\*(.*?)\*/g, '<i>$1</i>');
                formatted = formatted.replace(/_(.*?)_/g, '<i>$1</i>');
                formatted = formatted.replace(/~~(.*?)~~/g, '<s>$1</s>');
                formatted = formatted.replace(/\|\|(.*?)\|\|/g, '<span class="tg-spoiler" onclick="this.classList.toggle(\'revealed\')">$1</span>');
                escaped = formatted;
            }
            escaped = escaped.replace(/%%%CODEBLOCK(\d+)%%%/g, (match, index) => {
                const block = codeBlocks[parseInt(index)];
                if (!block) return match;
                const content = Security.escapeHtml(block.content);
                if (block.type === 'pre') {
                    return `<pre class="tg-code-block"><code>${content}</code></pre>`;
                } else {
                    return `<code class="tg-inline-code">${content}</code>`;
                }
            });
            escaped = escaped.replace(/%%%LINK(\d+)%%%/g, (match, index) => {
                const link = linkPlaceholders[parseInt(index)];
                if (!link) return match;
                return `<a href="${link.url}" target="_blank" rel="noopener noreferrer nofollow" class="tg-link">${link.content}</a>`;
            });
            escaped = escaped.replace(/^&gt;&gt;&gt; (.*)$/gm, '<blockquote class="tg-quote level-3">$1</blockquote>');
            escaped = escaped.replace(/^&gt;&gt; (.*)$/gm, '<blockquote class="tg-quote level-2">$1</blockquote>');
            escaped = escaped.replace(/^&gt; (.*)$/gm, '<blockquote class="tg-quote level-1">$1</blockquote>');
            const finalParts = [];
            lastIndex = 0;
            const tagRegex = /<[^>]+>/g;
            while ((match = tagRegex.exec(escaped)) !== null) {
                if (match.index > lastIndex) {
                    finalParts.push({ type: 'text', content: escaped.substring(lastIndex, match.index) });
                }
                finalParts.push({ type: 'tag', content: match[0] });
                lastIndex = match.index + match[0].length;
            }
            if (lastIndex < escaped.length) {
                finalParts.push({ type: 'text', content: escaped.substring(lastIndex) });
            }
            const wordChar = '[\\p{L}\\p{N}_]';
            const wordCharRegex = new RegExp(`(?<!\\w)@(${wordChar}+)`, 'gu');
            const hashtagRegex = new RegExp(`(?<!\\w)#(${wordChar}+)`, 'gu');
            escaped = finalParts.map(part => {
                if (part.type === 'tag') return part.content;
                return part.content
                    .replace(wordCharRegex, '<span class="tg-mention" data-mention="@$1">@$1</span>')
                    .replace(hashtagRegex, '<span class="tg-hashtag" data-hashtag="#$1">#$1</span>');
            }).join('');
            escaped = escaped.replace(/\n/g, '<br>');
            escaped = escaped.replace(/<a([^>]*)>\*\*(.*?)\*\*<\/a>/g, '<a$1><b>$2</b></a>');
            escaped = escaped.replace(/<a([^>]*)>\*(.*?)\*<\/a>/g, '<a$1><i>$2</i></a>');
            escaped = escaped.replace(/<a([^>]*)>__(.*?)__<\/a>/g, '<a$1><u>$2</u></a>');
            escaped = escaped.replace(/<a([^>]*)>~~(.*?)~~<\/a>/g, '<a$1><s>$2</s></a>');
            return escaped;
        }
    };

    const debounce = (fn, delay, options = {}) => {
        let timeoutId;
        let lastCall = 0;
        return function(...args) {
            const now = Date.now();
            if (options.leading && now - lastCall > delay) {
                fn.apply(this, args);
                lastCall = now;
            }
            clearTimeout(timeoutId);
            timeoutId = safeSetTimeout(() => {
                if (!options.leading || Date.now() - lastCall > delay) {
                    fn.apply(this, args);
                }
                timeoutId = null;
            }, delay);
        };
    };

    const throttle = (fn, limit) => {
        let inThrottle;
        return function(...args) {
            if (!inThrottle) {
                fn.apply(this, args);
                inThrottle = true;
                safeSetTimeout(() => inThrottle = false, limit);
            }
        };
    };

    const MessageAPI = {
        async fetchFullMessage(messageId) {
            if (!Security.validateMessageId(messageId)) return null;
            if (State.fullMessageCache.has(messageId)) {
                return State.fullMessageCache.get(messageId);
            }
            if (State.loadingMessages.has(messageId)) {
                return new Promise(resolve => {
                    const checkInterval = setInterval(() => {
                        if (State.fullMessageCache.has(messageId)) {
                            clearInterval(checkInterval);
                            resolve(State.fullMessageCache.get(messageId));
                        }
                    }, 100);
                    safeSetTimeout(() => {
                        clearInterval(checkInterval);
                        resolve(null);
                    }, 5000);
                });
            }
            State.loadingMessages.add(messageId);
            try {
                const url = `${CONFIG.API_BASE}/api/${CONFIG.API_VERSION}/messages/${messageId}?channel_id=${CONFIG.CHANNEL_ID}`;
                const response = await fetch(url);
                if (!response.ok) {
                    if (response.status === 404) {
                        return null;
                    }
                    throw new Error(`HTTP ${response.status}`);
                }
                const data = await response.json();
                State.fullMessageCache.set(messageId, data);
                if (State.fullMessageCache.size > CONFIG.MAX_CACHE_SIZE * 1.2) {
                    CacheManager.cleanup();
                }
                return data;
            } catch (err) {
                console.error(`Error fetching full message ${messageId}:`, err);
                return null;
            } finally {
                State.loadingMessages.delete(messageId);
            }
        },
        async fetchBatchMessages(messageIds) {
            if (!messageIds || messageIds.length === 0) return {};
            const neededIds = messageIds.filter(id => !State.fullMessageCache.has(id));
            if (neededIds.length === 0) {
                const result = {};
                messageIds.forEach(id => {
                    if (State.fullMessageCache.has(id)) {
                        result[id] = State.fullMessageCache.get(id);
                    }
                });
                return result;
            }
            try {
                const response = await fetch(`${CONFIG.API_BASE}/api/${CONFIG.API_VERSION}/messages/batch`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        channel_id: parseInt(CONFIG.CHANNEL_ID),
                        message_ids: neededIds
                    })
                });
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }
                const data = await response.json();
                if (data.messages) {
                    Object.entries(data.messages).forEach(([id, msg]) => {
                        State.fullMessageCache.set(parseInt(id), msg);
                    });
                }
                if (State.fullMessageCache.size > CONFIG.MAX_CACHE_SIZE * 1.2) {
                    CacheManager.cleanup();
                }
                const result = {};
                messageIds.forEach(id => {
                    if (State.fullMessageCache.has(id)) {
                        result[id] = State.fullMessageCache.get(id);
                    }
                });
                return result;
            } catch (err) {
                console.error('Error fetching batch messages:', err);
                return {};
            }
        },
        invalidateMessage(messageId) {
            State.fullMessageCache.delete(messageId);
        }
    };

    const API = {
        async fetchMessages(offset = 0, limit = CONFIG.INITIAL_LIMIT) {
            try {
                const response = await fetch(`${CONFIG.API_BASE}/api/channel/posts?channel_id=${CONFIG.CHANNEL_ID}&offset=${offset}&limit=${limit}`);
                if (!response.ok) throw new Error(`HTTP ${response.status}`);
                const data = await response.json();
                if (data.posts && data.posts.length > 0) {
                    const messageIds = data.posts.map(p => p.message_id);
                    const fullMessages = await MessageAPI.fetchBatchMessages(messageIds);
                    data.posts = data.posts.map(post => {
                        if (fullMessages[post.message_id]) {
                            return {
                                ...post,
                                ...fullMessages[post.message_id],
                                media_type: fullMessages[post.message_id].media_type || post.media_type,
                                has_media: !!(fullMessages[post.message_id].media_type || post.media_type)
                            };
                        }
                        return post;
                    });
                }
                return {
                    messages: data.posts || [],
                    hasMore: (data.posts || []).length === limit
                };
            } catch (err) {
                console.error('Error fetching messages:', err);
                return { messages: [], hasMore: false };
            }
        },
        async fetchMessagesSince(afterId, limit = 50) {
            try {
                const response = await fetch(`${CONFIG.API_BASE}/api/channel/posts/since?channel_id=${CONFIG.CHANNEL_ID}&after_id=${afterId}&limit=${limit}`);
                if (!response.ok) {
                    if (response.status === 404) return { posts: [] };
                    throw new Error(`HTTP ${response.status}`);
                }
                const data = await response.json();
                if (data.posts && data.posts.length > 0) {
                    const messageIds = data.posts.map(p => p.message_id);
                    const fullMessages = await MessageAPI.fetchBatchMessages(messageIds);
                    data.posts = data.posts.map(post => {
                        if (fullMessages[post.message_id]) {
                            return {
                                ...post,
                                ...fullMessages[post.message_id]
                            };
                        }
                        return post;
                    });
                }
                return data;
            } catch (err) {
                console.error('Error fetching messages since:', err);
                return { posts: [] };
            }
        },
        async fetchMediaOnce(messageId) {
            if (!Security.validateMessageId(messageId)) return null;
            if (State.mediaCache.has(messageId)) return State.mediaCache.get(messageId);
            const post = State.posts.get(messageId);
            if (post && post.media_pending === true) {
                return null;
            }
            try {
                const url = `${CONFIG.API_BASE}/api/media/by-message/${messageId}?channel_id=${CONFIG.CHANNEL_ID}&_t=${Date.now()}`;
                const response = await fetch(url);
                if (response.status === 404) {
                    State.mediaErrorCache.add(messageId);
                    return null;
                }
                if (!response.ok) throw new Error(`HTTP ${response.status}`);
                const media = await response.json();
                State.mediaCache.set(messageId, media);
                return media;
            } catch (err) {
                console.error(`Failed to load media for message ${messageId}`);
                State.mediaErrorCache.add(messageId);
                return null;
            }
        },
        async fetchMediaStatus(messageId) {
            if (!Security.validateMessageId(messageId)) return null;
            try {
                const url = `${CONFIG.API_BASE}/api/media/status/${messageId}?channel_id=${CONFIG.CHANNEL_ID}`;
                const response = await fetch(url);
                if (!response.ok) {
                    if (response.status === 404) return null;
                    throw new Error(`HTTP ${response.status}`);
                }
                return await response.json();
            } catch (err) {
                console.error(`Error fetching media status for ${messageId}:`, err);
                return null;
            }
        },
        clearMediaCache() {
            State.mediaCache.clear();
            State.mediaErrorCache.clear();
            State.mediaPending.clear();
            State.mediaRetryCount.clear();
            State.mediaRetryTimeouts.forEach(clearTimeout);
            State.mediaRetryTimeouts.clear();
            State.mediaStatusStore.clear();
            State.mediaReadyQueue.clear();
            State.failedMedia.clear();
            State.loadQueue = [];
            State.activeLoads = 0;
        }
    };

    const VideoManager = {
        stopAllVideos() {
            if (State.currentlyPlayingVideo) {
                State.currentlyPlayingVideo.pause();
                State.currentlyPlayingVideo = null;
            }
            document.querySelectorAll('video').forEach(video => {
                if (!video.paused) {
                    video.pause();
                }
            });
        },
        pauseVideo(video) {
            if (video && !video.paused) {
                video.pause();
                if (State.currentlyPlayingVideo === video) {
                    State.currentlyPlayingVideo = null;
                }
            }
        },
        playVideo(video) {
            if (!video) return;
            if (State.currentlyPlayingVideo && State.currentlyPlayingVideo !== video) {
                State.currentlyPlayingVideo.pause();
            }
            video.play().catch(() => {});
            State.currentlyPlayingVideo = video;
        },
        handleVideoPlay(video) {
            if (State.currentlyPlayingVideo && State.currentlyPlayingVideo !== video) {
                State.currentlyPlayingVideo.pause();
            }
            State.currentlyPlayingVideo = video;
        },
        handleVideoPause(video) {
            if (State.currentlyPlayingVideo === video) {
                State.currentlyPlayingVideo = null;
            }
        },
        abortAllLoads() {
            document.querySelectorAll('video[data-loading="true"]').forEach(video => {
                video.pause();
                video.removeAttribute('src');
                video.load();
                video.removeAttribute('data-loading');
            });
        }
    };

    const ThemeManager = {
        video: null,
        videoTimeoutId: null,
        init() {
            this.video = document.getElementById('bgVideo');
            if (this.video) {
                if (this.video.canPlayType) {
                    const canPlay = this.video.canPlayType('video/mp4');
                    if (canPlay === 'probably' || canPlay === 'maybe') {
                        this.video.load();
                        window.addEventListener('load', () => this.scheduleVideo());
                    }
                }
            }
            this.applyTheme(State.theme, false);
        },
        applyTheme(theme, animate = true) {
            if (animate) {
                document.documentElement.classList.add('theme-transitioning');
            }
            document.documentElement.setAttribute('data-theme', theme);
            if (theme === 'dark') {
                this.scheduleVideo();
            } else {
                this.hideVideo();
            }
            if (animate) {
                safeSetTimeout(() => {
                    document.documentElement.classList.remove('theme-transitioning');
                }, 400);
            }
        },
        scheduleVideo() {
            if (!this.video) return;
            if (this.videoTimeoutId) clearTimeout(this.videoTimeoutId);
            this.videoTimeoutId = safeSetTimeout(() => this.showVideo(), 10000);
        },
        showVideo() {
            if (this.video) {
                this.video.classList.add('visible');
                VideoManager.playVideo(this.video);
            }
        },
        hideVideo() {
            if (this.video) {
                this.video.classList.remove('visible');
                VideoManager.pauseVideo(this.video);
            }
            if (this.videoTimeoutId) {
                clearTimeout(this.videoTimeoutId);
                this.videoTimeoutId = null;
            }
        },
        toggle() {
            if (State.isTransitioning) {
                State.pendingTheme = State.theme === 'dark' ? 'light' : 'dark';
                return;
            }
            State.isTransitioning = true;
            this.hideVideo();
            const newTheme = State.theme === 'dark' ? 'light' : 'dark';
            requestAnimationFrame(() => {
                State.theme = newTheme;
                localStorage.setItem('theme', newTheme);
                this.applyTheme(newTheme, true);
                State.isTransitioning = false;
                if (State.pendingTheme) {
                    const temp = State.pendingTheme;
                    State.pendingTheme = null;
                    State.theme = temp;
                    this.toggle();
                }
            });
        }
    };

    const MediaManager = {
        replaceMediaContainer(postEl, html, messageId) {
            const old = postEl.querySelector('.media-container, .media-loading, .media-unavailable, .media-placeholder, .media-pending, .media-processing');
            if (!old) return null;
            old.outerHTML = html;
            const newContainer = postEl.querySelector('.media-container');
            if (newContainer) {
                newContainer.dataset.messageId = messageId;
            }
            return postEl.querySelector('.media-container');
        },
        updateMediaStatus(messageId, status, progress) {
            const postEl = document.querySelector(`.post[data-message-id="${messageId}"]`);
            if (!postEl) return false;
            const mediaContainer = postEl.querySelector('.media-container, .media-loading, .media-pending, .media-processing, .media-unavailable');
            if (!mediaContainer) {
                if (status !== 'ready') {
                    const postContent = postEl.querySelector('.post-content');
                    if (postContent) {
                        let statusHTML = '';
                        switch(status) {
                            case 'downloading':
                                statusHTML = `<div class="media-processing">📥 Downloading... ${progress}%</div>`;
                                break;
                            case 'processing':
                                statusHTML = `<div class="media-processing">⚙️ Processing... ${progress}%</div>`;
                                break;
                            case 'uploading':
                                statusHTML = `<div class="media-processing">☁️ Uploading to CDN... ${progress}%</div>`;
                                break;
                            case 'failed':
                                statusHTML = `<div class="media-unavailable">❌ Media failed to load</div>`;
                                break;
                            default:
                                statusHTML = `<div class="media-loading"><img src="/tg/core/loader.svg" alt="Loading" class="media-loader"></div>`;
                        }
                        postContent.insertAdjacentHTML('beforeend', statusHTML);
                    }
                }
                return true;
            }
            if (status === 'ready') {
                const post = State.posts.get(messageId);
                if (post && !post.media_url) {
                    State.mediaReadyQueue.add(messageId);
                    safeSetTimeout(() => {
                        if (State.mediaReadyQueue.has(messageId)) {
                            State.mediaReadyQueue.delete(messageId);
                            this.loadMedia(messageId);
                        }
                    }, CONFIG.MEDIA_READY_DELAY);
                }
            } else if (status === 'downloading' || status === 'processing' || status === 'uploading') {
                if (mediaContainer.classList.contains('media-loading') || mediaContainer.classList.contains('media-pending')) {
                    mediaContainer.outerHTML = `<div class="media-processing">${this.getStatusText(status)} ${progress}%</div>`;
                } else if (mediaContainer.classList.contains('media-processing')) {
                    mediaContainer.textContent = `${this.getStatusText(status)} ${progress}%`;
                }
            } else if (status === 'failed') {
                mediaContainer.outerHTML = '<div class="media-unavailable">❌ Media failed to load</div>';
                State.mediaErrorCache.add(messageId);
            }
            return true;
        },
        getStatusText(status) {
            switch(status) {
                case 'downloading': return '📥 Downloading';
                case 'processing': return '⚙️ Processing';
                case 'uploading': return '☁️ Uploading to CDN';
                default: return '⏳ Processing';
            }
        },
        processNextInQueue() {
            if (State.activeLoads >= CONFIG.MAX_CONCURRENT_LOADS || State.loadQueue.length === 0) {
                return;
            }
            const nextMessageId = State.loadQueue.shift();
            if (!nextMessageId) return;
            State.activeLoads++;
            this._performLoad(nextMessageId).finally(() => {
                State.activeLoads--;
                this.processNextInQueue();
            });
        },
        async _performLoad(messageId) {
            const post = State.posts.get(messageId);
            if (!post || !post.has_media || post.media_url) {
                return;
            }
            const timeoutPromise = new Promise((_, reject) => {
                safeSetTimeout(() => reject(new Error('Media load timeout')), CONFIG.MEDIA_LOAD_TIMEOUT);
            });
            try {
                const mediaInfo = await Promise.race([
                    API.fetchMediaOnce(messageId),
                    timeoutPromise
                ]);
                if (mediaInfo && mediaInfo.url) {
                    post.media_url = mediaInfo.url;
                    post.media_type = mediaInfo.file_type || post.media_type;
                    UI.updatePost(messageId, {
                        media_url: mediaInfo.url,
                        media_type: post.media_type
                    });
                    State.mediaRetryCount.delete(messageId);
                    State.mediaStatusStore.delete(messageId);
                    State.failedMedia.delete(messageId);
                } else if (mediaInfo && mediaInfo.error) {
                    throw new Error(mediaInfo.error);
                }
            } catch (error) {
                console.error(`Failed to load media for message ${messageId}:`, error);
                State.failedMedia.set(messageId, {
                    error: error.message,
                    timestamp: Date.now()
                });
                const currentRetries = State.mediaRetryCount.get(messageId) || 0;
                if (currentRetries < CONFIG.MEDIA_MAX_RETRIES) {
                    State.mediaRetryCount.set(messageId, currentRetries + 1);
                    safeSetTimeout(() => {
                        this.queueMediaLoad(messageId, true);
                    }, CONFIG.MEDIA_RETRY_DELAY * Math.pow(2, currentRetries));
                } else {
                    UI.updatePostMediaUnavailable(messageId, 'max_retries');
                    State.mediaRetryCount.delete(messageId);
                }
            }
        },
        queueMediaLoad(messageId, isRetry = false) {
            if (State.mediaLoading.has(messageId)) {
                return;
            }
            const post = State.posts.get(messageId);
            if (!post || !post.has_media) return;
            if (post.media_url) return;
            if (State.mediaErrorCache.has(messageId)) return;
            State.mediaLoading.add(messageId);
            if (!State.loadQueue.includes(messageId)) {
                State.loadQueue.push(messageId);
            }
            this.processNextInQueue();
        },
        loadMedia(messageId, isRetry = false) {
            if (State.mediaLoading.has(messageId)) {
                return;
            }
            const post = State.posts.get(messageId);
            if (!post || !post.has_media) return;
            if (post.media_url) return;
            if (State.mediaErrorCache.has(messageId)) return;
            State.mediaReadyQueue.delete(messageId);
            const statusInfo = State.mediaStatusStore.get(messageId);
            if (statusInfo) {
                this.updateMediaStatus(messageId, statusInfo.status, statusInfo.progress);
                if (statusInfo.status === 'processing' || statusInfo.status === 'uploading') {
                    safeSetTimeout(() => {
                        this.loadMedia(messageId);
                    }, CONFIG.MEDIA_STATUS_POLL_INTERVAL);
                    return;
                }
            }
            if (isRetry) {
                const postEl = document.querySelector(`.post[data-message-id="${messageId}"]`);
                if (postEl) {
                    const pendingEl = postEl.querySelector('.media-pending, .media-loading, .media-processing');
                    if (!pendingEl) {
                        const unavailableEl = postEl.querySelector('.media-unavailable');
                        if (unavailableEl) {
                            unavailableEl.outerHTML = '<div class="media-loading"><img src="/tg/core/loader.svg" alt="Loading" class="media-loader"></div>';
                        }
                    }
                }
            }
            this.queueMediaLoad(messageId, isRetry);
        },
        unloadMedia(messageId) {
            if (!CONFIG.IMAGE_UNLOAD_DISTANCE) return false;
            const postEl = document.querySelector(`.post[data-message-id="${messageId}"]`);
            if (!postEl) return false;
            const rect = postEl.getBoundingClientRect();
            const viewportHeight = window.innerHeight;
            const distanceFromViewport = Math.min(
                Math.abs(rect.top - viewportHeight),
                Math.abs(rect.bottom)
            );
            if (distanceFromViewport > CONFIG.IMAGE_UNLOAD_DISTANCE) {
                const mediaContainer = postEl.querySelector('.media-container');
                if (mediaContainer && !mediaContainer.classList.contains('media-unloaded')) {
                    const video = mediaContainer.querySelector('video');
                    if (video) {
                        VideoManager.pauseVideo(video);
                        video.dataset.src = video.src;
                        video.removeAttribute('src');
                        video.load();
                        mediaContainer.classList.add('media-unloaded');
                        if (!mediaContainer.querySelector('.media-placeholder')) {
                            const placeholder = document.createElement('div');
                            placeholder.className = 'media-placeholder';
                            placeholder.textContent = '📹';
                            mediaContainer.appendChild(placeholder);
                        }
                        const index = State.loadQueue.indexOf(parseInt(messageId));
                        if (index > -1) {
                            State.loadQueue.splice(index, 1);
                        }
                        State.mediaLoading.delete(parseInt(messageId));
                        return true;
                    }
                    const img = mediaContainer.querySelector('img');
                    if (img) {
                        img.dataset.src = img.src;
                        img.style.display = 'none';
                        mediaContainer.classList.add('media-unloaded');
                        if (!mediaContainer.querySelector('.media-placeholder')) {
                            const placeholder = document.createElement('div');
                            placeholder.className = 'media-placeholder';
                            placeholder.textContent = '📷';
                            mediaContainer.appendChild(placeholder);
                        }
                        const index = State.loadQueue.indexOf(parseInt(messageId));
                        if (index > -1) {
                            State.loadQueue.splice(index, 1);
                        }
                        State.mediaLoading.delete(parseInt(messageId));
                        return true;
                    }
                }
            }
            return false;
        },
        restoreMediaIfNeeded(messageId) {
            const postEl = document.querySelector(`.post[data-message-id="${messageId}"]`);
            if (!postEl) return false;
            const mediaContainer = postEl.querySelector('.media-container');
            if (mediaContainer && mediaContainer.classList.contains('media-unloaded')) {
                const post = State.posts.get(messageId);
                if (post && post.media_url) {
                    const placeholder = mediaContainer.querySelector('.media-placeholder');
                    if (placeholder) placeholder.remove();
                    const html = UI.renderMedia(post.media_url, post.media_type);
                    this.replaceMediaContainer(postEl, html, messageId);
                    const newVideo = postEl.querySelector('video');
                    if (newVideo && UI.isElementInViewport(postEl)) {
                        VideoManager.playVideo(newVideo);
                    }
                    return true;
                }
            }
            return false;
        },
        retryMedia(messageId) {
            if (State.mediaRetryTimeouts.has(messageId)) {
                clearTimeout(State.mediaRetryTimeouts.get(messageId));
                State.mediaRetryTimeouts.delete(messageId);
            }
            const retryCount = State.mediaRetryCount.get(messageId) || 0;
            if (retryCount >= CONFIG.MEDIA_MAX_RETRIES) {
                UI.updatePostMediaUnavailable(messageId, 'max_retries');
                State.mediaRetryCount.delete(messageId);
                return;
            }
            const delay = CONFIG.MEDIA_RETRY_DELAY * Math.pow(1.5, retryCount) + (Math.random() * 1000);
            const timeoutId = safeSetTimeout(() => {
                State.mediaRetryTimeouts.delete(messageId);
                this.loadMedia(messageId, true);
            }, delay);
            State.mediaRetryTimeouts.set(messageId, timeoutId);
        },
        handleMediaReady(messageId, mediaUrl, mediaType) {
            const post = State.posts.get(messageId);
            if (post) {
                let displayMediaType = mediaType;
                if (displayMediaType === 'webpage_preview') {
                    displayMediaType = 'photo';
                }
                post.media_url = mediaUrl;
                post.media_type = displayMediaType || post.media_type;
                UI.updatePost(messageId, {
                    media_url: mediaUrl,
                    media_type: post.media_type
                });
            }
            State.mediaPending.delete(messageId);
            State.mediaLoading.delete(messageId);
            State.mediaRetryCount.delete(messageId);
            State.mediaStatusStore.delete(messageId);
            State.mediaReadyQueue.delete(messageId);
            State.failedMedia.delete(messageId);
            const index = State.loadQueue.indexOf(messageId);
            if (index > -1) {
                State.loadQueue.splice(index, 1);
            }
            if (State.mediaRetryTimeouts.has(messageId)) {
                clearTimeout(State.mediaRetryTimeouts.get(messageId));
                State.mediaRetryTimeouts.delete(messageId);
            }
            State.mediaCache.set(messageId, { url: mediaUrl, file_type: mediaType });
            if (State.mediaCache.size > CONFIG.MAX_MEDIA_CACHE_SIZE * 1.2) {
                CacheManager.cleanup();
            }
        },
        handleMediaStatus(messageId, status, progress, mediaId) {
            const post = State.posts.get(messageId);
            if (!post || !post.has_media) return;
            State.mediaStatusStore.set(messageId, {
                status,
                progress,
                mediaId,
                lastUpdate: Date.now()
            });
            this.updateMediaStatus(messageId, status, progress);
            if (status === 'ready') {
                safeSetTimeout(() => {
                    this.loadMedia(messageId);
                }, 500);
            }
        },
        showMediaPending(messageId) {
            const postEl = document.querySelector(`.post[data-message-id="${messageId}"]`);
            if (!postEl) return;
            const container = postEl.querySelector('.media-loading, .media-container, .media-unavailable, .media-pending');
            if (container) {
                container.outerHTML = '<div class="media-pending">⏳ Media processing...</div>';
            } else {
                const postContent = postEl.querySelector('.post-content');
                if (postContent) {
                    postContent.insertAdjacentHTML('beforeend', '<div class="media-pending">⏳ Media processing...</div>');
                }
            }
        }
    };

    const UI = {
        observer: null,
        initIntersectionObserver() {
            if (this.observer) {
                this.observer.disconnect();
            }
            this.observer = new IntersectionObserver((entries) => {
                entries.forEach(entry => {
                    const post = entry.target;
                    const msgId = Number(post.dataset.messageId);
                    if (entry.isIntersecting) {
                        State.visiblePosts.add(msgId);
                        MediaManager.loadMedia(msgId);
                        MediaManager.restoreMediaIfNeeded(msgId);
                        const video = post.querySelector('video');
                        if (video && video.dataset.src && !video.src) {
                            video.src = video.dataset.src;
                            delete video.dataset.src;
                            VideoManager.playVideo(video);
                        }
                    } else {
                        State.visiblePosts.delete(msgId);
                        const video = post.querySelector('video');
                        if (video && !video.paused) {
                            VideoManager.pauseVideo(video);
                        }
                        MediaManager.unloadMedia(msgId);
                    }
                });
            }, {
                rootMargin: `${CONFIG.LAZY_LOAD_OFFSET}px`,
                threshold: 0.01
            });
            document.querySelectorAll('.post').forEach(post => {
                this.observer.observe(post);
            });
        },
        cacheDOM() {
            const posts = document.querySelectorAll('.post');
            if (posts.length === 0) return;
            const postsToCache = Array.from(posts).slice(-20);
            const fragment = document.createDocumentFragment();
            postsToCache.forEach(post => {
                fragment.appendChild(post.cloneNode(true));
            });
            State.domCache = fragment;
            State.scrollPosition = window.scrollY;
        },
        restoreDOM() {
            if (!State.domCache) return false;
            const feed = document.getElementById('feed');
            feed.innerHTML = '';
            feed.appendChild(State.domCache.cloneNode(true));
            feed.querySelectorAll('.post').forEach(post => {
                const originalPost = State.posts.get(Number(post.dataset.messageId));
                if (originalPost) {
                    post.dataset.mediaUrl = originalPost.media_url || '';
                    post.dataset.mediaType = originalPost.media_type || '';
                }
                requestAnimationFrame(() => {
                    post.classList.add('visible');
                });
            });
            this.initIntersectionObserver();
            return true;
        },
        isElementInViewport(el) {
            const rect = el.getBoundingClientRect();
            return (
                rect.top >= 0 &&
                rect.left >= 0 &&
                rect.bottom <= (window.innerHeight || document.documentElement.clientHeight) &&
                rect.right <= (window.innerWidth || document.documentElement.clientWidth)
            );
        },
        trimOldPosts() {
            const posts = document.querySelectorAll('.post');
            if (posts.length > CONFIG.MAX_VISIBLE_POSTS) {
                const toRemove = Array.from(posts).slice(0, posts.length - CONFIG.MAX_VISIBLE_POSTS);
                toRemove.forEach(el => {
                    if (this.observer) {
                        this.observer.unobserve(el);
                    }
                    const video = el.querySelector('video');
                    if (video) VideoManager.pauseVideo(video);
                    el.remove();
                });
            }
        },
        updateChannelInfo() {
            document.getElementById('channelTitle').textContent = CONFIG.CHANNEL_TITLE;
            document.getElementById('channelUsername').textContent = `@${CONFIG.CHANNEL_USERNAME}`;
            const avatarEl = document.getElementById('channelAvatar');
            if (avatarEl) {
                avatarEl.innerHTML = `<img src="avatar.jpg" style="width:54px; height:54px; object-fit:cover;" alt="Channel avatar">`;
            }
        },
        updateConnectionStatus(connected) {
            const dot = document.getElementById('statusDot');
            dot.classList.toggle('offline', !connected);
        },
        updateNewPostsBadge() {
            const badge = document.getElementById('newPostsBadge');
            const countSpan = document.getElementById('newPostsCount');
            if (State.newPosts.length > 0) {
                countSpan.textContent = State.newPosts.length;
                badge.classList.remove('hidden');
            } else {
                badge.classList.add('hidden');
            }
        },
        showSkeletonLoaders() {
            const feed = document.getElementById('feed');
            feed.innerHTML = '';
            for (let i = 0; i < 3; i++) {
                const skeleton = document.createElement('div');
                skeleton.className = 'skeleton';
                feed.appendChild(skeleton);
            }
        },
        renderMedia(url, type) {
            if (!url) return '';
            const fullUrl = url.startsWith('http') ? url : `${CONFIG.API_BASE}${url}`;
            let isVideo = false;
            let typeStr = '';
            if (type) {
                typeStr = String(type).toLowerCase();
                isVideo = typeStr.includes('video') ||
                         typeStr.includes('document') ||
                         typeStr.includes('animation') ||
                         typeStr === 'messagemediadocument' ||
                         typeStr.includes('gif') ||
                         typeStr.includes('mp4') ||
                         typeStr.includes('webm') ||
                         typeStr.includes('mov');
                if (typeStr === 'webpage_preview') {
                    isVideo = false;
                }
                if (!isVideo && fullUrl.match(/\.(mp4|webm|mov|gif)$/i)) {
                    isVideo = true;
                }
            } else if (fullUrl.match(/\.(mp4|webm|mov|gif)$/i)) {
                isVideo = true;
            }
            if (isVideo) {
                const isGifLike = fullUrl.match(/\.gif$/i) ||
                    (type && (typeStr.includes('gif') || typeStr.includes('animation'))) ||
                    fullUrl.includes('/gif/') ||
                    fullUrl.includes('_gif.') ||
                    fullUrl.includes('.gif?');
                const videoId = `video-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
                if (isGifLike) {
                    return `
                        <div class="media-container">
                            <video
                                id="${videoId}"
                                src="${fullUrl}"
                                autoplay
                                loop
                                muted
                                playsinline
                                preload="${CONFIG.VIDEO_PRELOAD}"
                                style="max-width:100%; max-height:500px; background:#282c3000;"
                                data-message-id="${State.posts.size}">
                                Your browser does not support video.
                            </video>
                        </div>
                    `;
                } else {
                    return `
                        <div class="media-container">
                            <video
                                id="${videoId}"
                                src="${fullUrl}"
                                controls
                                preload="${CONFIG.VIDEO_PRELOAD}"
                                playsinline
                                style="max-width:100%; max-height:500px; background:#282c3000;"
                                data-message-id="${State.posts.size}">
                                Your browser does not support video.
                            </video>
                        </div>
                    `;
                }
            } else {
                const imgId = `img-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
                return `
                    <div class="media-container" data-media-type="image">
                        <img
                            id="${imgId}"
                            src="${fullUrl}"
                            alt="Media"
                            loading="lazy"
                            decoding="async"
                            data-message-id="${State.posts.size}">
                    </div>
                `;
            }
        },
        attachMediaHandlers(postEl) {
            const videos = postEl.querySelectorAll('video');
            videos.forEach(video => {
                video.addEventListener('play', () => VideoManager.handleVideoPlay(video));
                video.addEventListener('pause', () => VideoManager.handleVideoPause(video));
                video.addEventListener('error', (e) => {
                    console.error(`Video failed to load: ${video.src}`, e);
                    const error = video.error;
                    if (error && error.code === MediaError.MEDIA_ERR_NETWORK) {
                        const messageId = video.closest('.post')?.dataset.messageId;
                        if (messageId && CONFIG.RETRY_ON_NETWORK_ERROR) {
                            safeSetTimeout(() => {
                                video.load();
                                video.play().catch(() => {});
                            }, 5000);
                        }
                    } else {
                        const container = video.closest('.media-container');
                        if (container && !container.querySelector('.media-error')) {
                            container.innerHTML = '<div class="media-error">Failed to load video</div>';
                        }
                    }
                });
                if (navigator.onLine === false) {
                    video.setAttribute('data-pending', 'true');
                }
            });
            const images = postEl.querySelectorAll('img');
            images.forEach(img => {
                img.addEventListener('error', () => {
                    console.error('Image failed to load:', img.src);
                    const container = img.closest('.media-container');
                    if (container && !container.querySelector('.media-error')) {
                        container.innerHTML = '<div class="media-error">Failed to load image</div>';
                    }
                });
            });
        },
        handleNetworkOnline() {
            console.log('Network is online, retrying failed videos');
            document.querySelectorAll('video[data-pending="true"]').forEach(video => {
                video.removeAttribute('data-pending');
                video.load();
                video.play().catch(() => {});
            });
            State.failedMedia.forEach((_, messageId) => {
                if (State.mediaRetryCount.get(messageId) || 0 < CONFIG.MEDIA_MAX_RETRIES) {
                    MediaManager.loadMedia(messageId, true);
                }
            });
        },
        
        createPostElement(post) {
            const postEl = document.createElement('div');
            postEl.className = 'post';
            postEl.dataset.messageId = post.message_id;
            postEl.dataset.mediaUrl = post.media_url || '';
            postEl.dataset.mediaType = post.media_type || '';
            const date = Formatters.formatDate(post.date);
            const views = Formatters.formatViews(post.views);
            const text = Formatters.formatText(post.text);
            let mediaHTML = '';
            if (post.media_url) {
                mediaHTML = this.renderMedia(post.media_url, post.media_type);
            } else if (post.has_media) {
                const statusInfo = State.mediaStatusStore.get(post.message_id);
                if (statusInfo) {
                    switch(statusInfo.status) {
                        case 'downloading':
                            mediaHTML = `<div class="media-processing">Downloading... ${statusInfo.progress}%</div>`;
                            break;
                        case 'processing':
                            mediaHTML = `<div class="media-processing">Processing... ${statusInfo.progress}%</div>`;
                            break;
                        case 'uploading':
                            mediaHTML = `<div class="media-processing">Uploading to CDN... ${statusInfo.progress}%</div>`;
                            break;
                        case 'failed':
                            mediaHTML = '<div class="media-unavailable">Media failed to load</div>';
                            break;
                        case 'ready':
                            mediaHTML = '<div class="media-loading"><img src="/tg/core/loader.svg" alt="Loading" class="media-loader"></div>';
                            State.mediaReadyQueue.add(post.message_id);
                            safeSetTimeout(() => {
                                if (State.mediaReadyQueue.has(post.message_id)) {
                                    State.mediaReadyQueue.delete(post.message_id);
                                    MediaManager.loadMedia(post.message_id);
                                }
                            }, 500);
                            break;
                        default:
                            mediaHTML = '<div class="media-loading"><img src="/tg/core/loader.svg" alt="Loading" class="media-loader"></div>';
                    }
                } else {
                    const retryCount = State.mediaRetryCount.get(post.message_id) || 0;
                    if (retryCount >= CONFIG.MEDIA_MAX_RETRIES) {
                        mediaHTML = '<div class="media-unavailable">Media unavailable</div>';
                    } else {
                        mediaHTML = '<div class="media-loading"><img src="/tg/core/loader.svg" alt="Loading" class="media-loader"></div>';
                    }
                }
            }
            
            const heartSvg = `
                <svg class="heartSVG" version="1.1" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" viewBox="0 0 24 24" width="24" height="24">
                    <path class="heart" fill="#ABB9C2" d="M12,21.35L10.55,20.03C5.4,15.36 2,12.27 2,8.5 2,5.41 4.42,3 7.5,3c1.74,0 3.41.81 4.5,2.08C13.09,3.81 14.76,3 16.5,3 19.58,3 22,5.41 22,8.5c0,3.77-3.4,6.86-8.55,11.54L12,21.35Z"/>
                </svg>
            `;
            
            postEl.innerHTML = `
                <div class="post-content">
                    <div class="post-header">
                        <div class="post-avatar">
                            <img src="avatar.jpg" style="width:36px; height:36px; object-fit:cover;" alt="Channel avatar">
                        </div>
                        <div class="post-author-info">
                            <div class="post-author-name">
                                ${CONFIG.CHANNEL_TITLE}
                            </div>
                            <div class="post-date">
                                ${date}
                            </div>
                        </div>
                    </div>
                    <div class="post-text">${text || '<i></i>'}</div>
                    ${mediaHTML}
                </div>
                <div class="post-footer style="display:inline-flex; align-items:center;">
                    <button class="heart-btn" data-message-id="${post.message_id}" data-liked="false" data-raw-views="${post.views}" style="background:none; border:none; cursor:pointer; padding:5px; display:inline-flex; align-items:flex-end; gap:6px;">
                        ${heartSvg}
                        <span class="views-count" style="font-size:12px;">${views}</span>
                    </button>
                </div>
            `;
            setTimeout(() => this.attachMediaHandlers(postEl), 0);
            
            const heartBtn = postEl.querySelector('.heart-btn');
            if (heartBtn) {
                const heartIcon = heartBtn.querySelector('.heart');
                
                heartBtn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    
                    const isLiked = heartBtn.getAttribute('data-liked') === 'true';
                    const viewsSpan = heartBtn.querySelector('.views-count');
                    let rawViews = parseInt(heartBtn.getAttribute('data-raw-views')) || 0;
                    
                    if (!isLiked) {
                        heartBtn.setAttribute('data-liked', 'true');
                        rawViews++;
                        heartIcon.setAttribute('fill', '#E00050');
                        if (navigator.vibrate) navigator.vibrate(50);
                    } else {
                        heartBtn.setAttribute('data-liked', 'false');
                        rawViews--;
                        heartIcon.setAttribute('fill', '#ABB9C2');
                    }
                    
                    heartBtn.setAttribute('data-raw-views', rawViews);
                    viewsSpan.textContent = Formatters.formatViews(rawViews);
                });
            }
            
            return postEl;
        },

        updatePost(messageId, data, options = {}) {
            const postEl = document.querySelector(`.post[data-message-id="${messageId}"]`);
            if (!postEl) {
                if (data.media_url) {
                    State.mediaPending.set(messageId, {
                        media_url: data.media_url,
                        media_type: data.media_type,
                        timestamp: Date.now()
                    });
                }
                return false;
            }
            const currentPost = State.posts.get(Number(messageId));
            if (currentPost) {
                const hasChanges =
                    (data.edit_date && currentPost.edit_date !== data.edit_date) ||
                    (data.text !== undefined && currentPost.text !== data.text) ||
                    (data.views !== undefined && currentPost.views !== data.views) ||
                    (data.forwards !== undefined && currentPost.forwards !== data.forwards) ||
                    (data.media_url && currentPost.media_url !== data.media_url);
                if (!hasChanges) {
                    return false;
                }
                State.posts.set(Number(messageId), {...currentPost, ...data});
            }
            let changed = false;
            if (data.text !== undefined) {
                const textEl = postEl.querySelector('.post-text');
                if (textEl) {
                    const newText = Formatters.formatText(data.text || '');
                    if (textEl.innerHTML !== newText) {
                        textEl.innerHTML = newText;
                        changed = true;
                    }
                }
            }
            if (data.edit_date) {
                const dateEl = postEl.querySelector('.post-date');
                if (dateEl) {
                    const newDate = Formatters.formatDate(data.edit_date);
                    if (!dateEl.innerHTML.includes(newDate)) {
                        dateEl.innerHTML = newDate;
                        if (!dateEl.innerHTML.includes('(edited)')) {
                            dateEl.innerHTML += ' ';
                        }
                        changed = true;
                    }
                }
            }
            if (data.views !== undefined) {
                const viewsSpan = postEl.querySelector('.heart-btn .views-count');
                if (viewsSpan) {
                    const newViews = Formatters.formatViews(data.views);
                    if (viewsSpan.textContent !== newViews) {
                        viewsSpan.textContent = newViews;
                        const heartBtn = postEl.querySelector('.heart-btn');
                        if (heartBtn) {
                            heartBtn.setAttribute('data-raw-views', data.views);
                            const isLiked = heartBtn.getAttribute('data-liked') === 'true';
                            if (isLiked) {
                                let rawViews = data.views;
                                rawViews++;
                                heartBtn.setAttribute('data-raw-views', rawViews);
                                viewsSpan.textContent = Formatters.formatViews(rawViews);
                            }
                        }
                        changed = true;
                    }
                }
            }
            if (data.forwards !== undefined) {
                const forwardsEl = postEl.querySelector('.forwards-count');
                if (forwardsEl) {
                    const newForwards = `🔁 ${data.forwards}`;
                    if (forwardsEl.textContent !== newForwards) {
                        forwardsEl.textContent = newForwards;
                        changed = true;
                    }
                }
            }
            if (data.media_url) {
                const mediaContainer = postEl.querySelector('.media-container, .media-loading, .media-unavailable, .media-pending, .media-processing');
                if (mediaContainer) {
                    const video = mediaContainer.querySelector('video');
                    if (video) VideoManager.pauseVideo(video);
                    const newMedia = this.renderMedia(data.media_url, data.media_type);
                    MediaManager.replaceMediaContainer(postEl, newMedia, messageId);
                    postEl.dataset.mediaUrl = data.media_url;
                    postEl.dataset.mediaType = data.media_type || '';
                    changed = true;
                    setTimeout(() => this.attachMediaHandlers(postEl), 0);
                } else {
                    const postContent = postEl.querySelector('.post-content');
                    if (postContent) {
                        const newMedia = this.renderMedia(data.media_url, data.media_type);
                        postContent.insertAdjacentHTML('beforeend', newMedia);
                        postEl.dataset.mediaUrl = data.media_url;
                        postEl.dataset.mediaType = data.media_type || '';
                        changed = true;
                        setTimeout(() => this.attachMediaHandlers(postEl), 0);
                    }
                }
            }
            if (changed && !options.batch) {
                postEl.classList.add('updated');
                safeSetTimeout(() => postEl.classList.remove('updated'), 2000);
            }
            return changed;
        },
        updatePostQuiet(messageId, data) {
            return this.updatePost(messageId, data, { batch: true });
        },
        updatePostMediaUnavailable(messageId, reason = 'failed') {
            const postEl = document.querySelector(`.post[data-message-id="${messageId}"]`);
            if (!postEl) return false;
            const mediaContainer = postEl.querySelector('.media-loading, .media-pending, .media-container, .media-processing');
            if (mediaContainer) {
                const video = mediaContainer.querySelector('video');
                if (video) VideoManager.pauseVideo(video);
                let message = '📷 Media unavailable';
                switch(reason) {
                    case 'timeout':
                        message = '⏱️ Media took too long to load';
                        break;
                    case 'not_found':
                        message = '📷 Media not found';
                        break;
                    case 'max_retries':
                        message = '📷 Media failed to load after multiple attempts';
                        break;
                    case 'error':
                        message = '📷 Error loading media';
                        break;
                }
                mediaContainer.outerHTML = `<div class="media-unavailable">${message}</div>`;
                State.mediaErrorCache.add(Number(postEl.dataset.messageId));
                return true;
            }
            return false;
        },
        deletePost(messageId) {
            const postEl = document.querySelector(`.post[data-message-id="${messageId}"]`);
            if (!postEl) return false;
            if (this.observer) {
                this.observer.unobserve(postEl);
            }
            const video = postEl.querySelector('video');
            if (video) VideoManager.pauseVideo(video);
            postEl.classList.add('deleted');
            safeSetTimeout(() => {
                postEl.remove();
                State.posts.delete(messageId);
                const index = State.postOrder.indexOf(Number(messageId));
                if (index !== -1) State.postOrder.splice(index, 1);
                State.mediaPending.delete(messageId);
                State.mediaLoading.delete(messageId);
                State.mediaRetryCount.delete(messageId);
                State.mediaStatusStore.delete(messageId);
                State.mediaReadyQueue.delete(messageId);
                State.fullMessageCache.delete(messageId);
                State.failedMedia.delete(messageId);
                const queueIndex = State.loadQueue.indexOf(Number(messageId));
                if (queueIndex > -1) {
                    State.loadQueue.splice(queueIndex, 1);
                }
            }, 300);
            return true;
        },
        renderPosts(posts) {
            const feed = document.getElementById('feed');
            const fragment = document.createDocumentFragment();
            posts.forEach(post => {
                const postEl = this.createPostElement(post);
                fragment.appendChild(postEl);
            });
            feed.appendChild(fragment);
            feed.querySelectorAll('.post').forEach(post => {
                requestAnimationFrame(() => post.classList.add('visible'));
            });
            if (this.observer) {
                feed.querySelectorAll('.post').forEach(post => {
                    this.observer.observe(post);
                });
            }
            this.trimOldPosts();
            posts.forEach(post => {
                if (post.has_media && !post.media_url) {
                    safeSetTimeout(() => {
                        MediaManager.loadMedia(post.message_id);
                    }, 500);
                }
            });
        },
        addPostToTop(post) {
            const messageId = post.message_id;
            const existingEl = document.querySelector(`.post[data-message-id="${messageId}"]`);
            if (existingEl) {
                if (State.posts.has(messageId)) {
                    const currentPost = State.posts.get(messageId);
                    if (currentPost.text !== post.text ||
                        currentPost.media_url !== post.media_url ||
                        currentPost.views !== post.views) {
                        this.updatePost(messageId, post);
                    }
                }
                return;
            }
            if (State.posts.has(messageId)) {
                State.posts.delete(messageId);
                const index = State.postOrder.indexOf(messageId);
                if (index !== -1) State.postOrder.splice(index, 1);
            }
            const queueIndex = State.newPosts.findIndex(p => p.message_id === messageId);
            if (queueIndex !== -1) {
                State.newPosts.splice(queueIndex, 1);
            }
            const feed = document.getElementById('feed');
            const postEl = this.createPostElement(post);
            let inserted = false;
            for (const child of feed.children) {
                const childId = parseInt(child.dataset.messageId);
                if (childId < messageId) {
                    feed.insertBefore(postEl, child);
                    inserted = true;
                    break;
                }
            }
            if (!inserted) {
                if (feed.firstChild) {
                    feed.insertBefore(postEl, feed.firstChild);
                } else {
                    feed.appendChild(postEl);
                }
            }
            State.posts.set(messageId, {...post});
            if (!State.postOrder.includes(messageId)) {
                State.postOrder.unshift(messageId);
                State.postOrder.sort((a, b) => b - a);
            }
            postEl.offsetHeight;
            requestAnimationFrame(() => {
                postEl.classList.add('visible', 'new');
            });
            safeSetTimeout(() => postEl.classList.remove('new'), 3000);
            if (this.observer) {
                this.observer.observe(postEl);
            }
            this.trimOldPosts();
            if (post.has_media && !post.media_url) {
                safeSetTimeout(() => {
                    MediaManager.loadMedia(post.message_id);
                }, 500);
            }
        },
        setLoaderVisible(visible) {
            const trigger = document.getElementById('infiniteScrollTrigger');
            if (trigger) {
                trigger.textContent = visible ? 'Loading...' : '↓ Load more';
            }
        },
        showScrollTopButton(visible) {
            const btn = document.getElementById('scrollTopBtn');
            if (btn) btn.style.display = visible ? 'flex' : 'none';
        },
        cleanup() {
            if (this.observer) {
                this.observer.disconnect();
                this.observer = null;
            }
            VideoManager.stopAllVideos();
            VideoManager.abortAllLoads();
            API.clearMediaCache();
        }
    };

    const Lightbox = {
        activeVideo: null,
        open(url, type) {
            if (!url) return;
            VideoManager.stopAllVideos();
            const lightbox = document.getElementById('lightbox');
            const content = document.getElementById('lightboxContent');
            const fullUrl = url.startsWith('http') ? url : `${CONFIG.API_BASE}${url}`;
            const isVideo = type === 'video' || type === 'Video' || url.match(/\.(mp4|webm|mov)$/i);
            if (isVideo) {
                const videoId = `lightbox-video-${Date.now()}`;
                content.innerHTML = `<video id="${videoId}" src="${fullUrl}" controls autoplay playsinline preload="auto"></video>`;
                this.activeVideo = content.querySelector('video');
                if (this.activeVideo) {
                    this.activeVideo.addEventListener('play', () => VideoManager.handleVideoPlay(this.activeVideo));
                    this.activeVideo.addEventListener('pause', () => VideoManager.handleVideoPause(this.activeVideo));
                    this.activeVideo.addEventListener('error', (e) => {
                        console.error('Lightbox video failed:', e);
                        content.innerHTML = '<div class="media-error">Failed to load video</div>';
                    });
                }
            } else {
                const imgId = `lightbox-img-${Date.now()}`;
                content.innerHTML = `<img id="${imgId}" src="${fullUrl}" alt="Media">`;
                const img = content.querySelector('img');
                if (img) {
                    img.addEventListener('error', () => {
                        content.innerHTML = '<div class="media-error">Failed to load image</div>';
                    });
                }
            }
            lightbox.classList.add('active');
            document.body.style.overflow = 'hidden';
        },
        close() {
            const lightbox = document.getElementById('lightbox');
            lightbox.classList.remove('active');
            if (this.activeVideo) {
                VideoManager.pauseVideo(this.activeVideo);
                this.activeVideo = null;
            }
            document.getElementById('lightboxContent').innerHTML = '';
            document.body.style.overflow = '';
        }
    };

    const MessageLoader = {
        async loadMessages(reset = false) {
            if (State.isLoading) return;
            if (reset) {
                UI.cacheDOM();
                UI.cleanup();
                State.posts.clear();
                State.postOrder = [];
                State.mediaPending.clear();
                State.mediaLoading.clear();
                State.mediaRetryCount.clear();
                State.mediaStatusStore.clear();
                State.mediaReadyQueue.clear();
                State.mediaRetryTimeouts.forEach(clearTimeout);
                State.mediaRetryTimeouts.clear();
                document.getElementById('feed').innerHTML = '';
                State.offset = 0;
                State.hasMore = true;
                API.clearMediaCache();
                State.failedMedia.clear();
                State.loadQueue = [];
                State.activeLoads = 0;
            }
            if (!State.hasMore) {
                document.getElementById('infiniteScrollTrigger').style.display = 'none';
                return;
            }
            State.isLoading = true;
            UI.setLoaderVisible(true);
            try {
                const data = await API.fetchMessages(State.offset, CONFIG.INITIAL_LIMIT);
                if (data.messages && data.messages.length > 0) {
                    State.hasMore = data.hasMore !== false;
                    State.offset += data.messages.length;
                    const newMessages = [];
                    data.messages.forEach(post => {
                        if (!State.posts.has(post.message_id)) {
                            State.posts.set(post.message_id, {...post});
                            State.postOrder.push(post.message_id);
                            newMessages.push(post);
                        }
                    });
                    State.postOrder.sort((a, b) => b - a);
                    if (newMessages.length > 0) {
                        UI.renderPosts(newMessages);
                    }
                    data.messages.forEach(post => {
                        if (post.has_media && !State.mediaErrorCache.has(post.message_id)) {
                            MediaManager.loadMedia(post.message_id);
                        }
                    });
                } else {
                    State.hasMore = false;
                }
            } catch (err) {
                console.error('Error loading messages:', err);
            } finally {
                State.isLoading = false;
                UI.setLoaderVisible(false);
            }
        },
        async loadInitial() {
            UI.showSkeletonLoaders();
            await this.loadMessages(true);
            UI.initIntersectionObserver();
        }
    };

    async function loadInitialAndProcessPending() {
        UI.showSkeletonLoaders();
        await MessageLoader.loadMessages(true);
        State.initialLoadComplete = true;
        while (State.pendingEvents.length > 0) {
            const event = State.pendingEvents.shift();
            WebSocketManager.processFullMessage(event.data, event.type);
        }
        UI.initIntersectionObserver();
    }

    const WebSocketManager = {
        giveUp: false,
        giveUpTimer: null,
        connect(wsUrl = CONFIG.WS_BASE) {
            if (!this.giveUpTimer) {
                this.giveUpTimer = safeSetTimeout(() => {
                    this.giveUp = true;
                }, CONFIG.RECONNECT_GIVE_UP_DELAY);
            }
            try {
                State.ws = new WebSocket(wsUrl);
                State.ws.onopen = () => {
                    State.wsConnected = true;
                    State.wsReconnectAttempts = 0;
                    if (this.giveUpTimer) {
                        clearTimeout(this.giveUpTimer);
                        this.giveUpTimer = null;
                    }
                    this.giveUp = false;
                    UI.updateConnectionStatus(true);
                    if (CONFIG.CHANNEL_ID) {
                        const subscribeMsg = {
                            type: 'subscribe',
                            channel_id: parseInt(CONFIG.CHANNEL_ID)
                        };
                        State.ws.send(JSON.stringify(subscribeMsg));
                    }
                    if (CONFIG.SYNC_AFTER_RECONNECT && State.postOrder.length > 0) {
                        this.syncAfterReconnect();
                    }
                    State.wsPingInterval = safeSetInterval(() => {
                        if (State.ws && State.ws.readyState === WebSocket.OPEN) {
                            State.ws.send(JSON.stringify({ type: 'ping' }));
                        }
                    }, CONFIG.PING_INTERVAL);
                };
                State.ws.onmessage = (event) => {
                    try {
                        const data = JSON.parse(event.data);
                        this.handleMessage(data);
                    } catch (err) {
                        console.error('Error parsing WebSocket message:', err);
                    }
                };
                State.ws.onclose = () => {
                    State.wsConnected = false;
                    UI.updateConnectionStatus(false);
                    if (State.wsPingInterval) {
                        clearInterval(State.wsPingInterval);
                        State.wsPingInterval = null;
                    }
                    if (!this.giveUp) {
                        this.reconnect();
                    }
                };
                State.ws.onerror = (err) => {
                    console.error('WebSocket error:', err);
                };
            } catch (err) {
                console.error('WebSocket connection error:', err);
                if (!this.giveUp) {
                    this.reconnect();
                }
            }
        },
        async syncAfterReconnect() {
            const lastPostId = State.postOrder[0];
            if (!lastPostId) return;
            try {
                const data = await API.fetchMessagesSince(lastPostId, 50);
                if (data.posts && data.posts.length > 0) {
                    const newPosts = data.posts.reverse();
                    newPosts.forEach(post => {
                        this.processFullMessage(post, 'new');
                    });
                }
            } catch (err) {
                console.error('Sync after reconnect failed:', err);
            }
        },
        handleMessage(data) {
            if (data.type === 'welcome') {
                if (data.version && State.supportedVersions.includes(data.version)) {
                    State.serverVersion = data.version;
                    console.log(`Connected to server v${data.version}`);
                }
                return;
            }
            if (data.event_id && data.event_id > State.lastEventId) {
                State.lastEventId = data.event_id;
            }
            if (data.type === 'event_batch') {
                this.handleEventBatch(data);
                return;
            }
            if (['ping', 'pong', 'heartbeat', 'buffering',
                 'flush_start', 'flush_complete', 'subscribed', 'error'].includes(data.type)) {
                return;
            }
            if (data.version && !State.supportedVersions.includes(data.version)) {
                return;
            }
            if (data.channel_id !== parseInt(CONFIG.CHANNEL_ID)) return;
            if (!State.initialLoadComplete) {
                State.pendingEvents.push({
                    data: data.data || data,
                    type: data.type
                });
                return;
            }
            const messageKey = `${data.channel_id}-${data.message_id}-${data.type}`;
            const lastReceived = State.recentMessages.get(messageKey);
            const ttl = data.type === 'edit' ? 2000 : CONFIG.DEDUP_TTL;
            if (lastReceived && (Date.now() - lastReceived < ttl)) {
                return;
            }
            State.recentMessages.set(messageKey, Date.now());
            this.handleSingleEvent(data);
        },
        handleSingleEvent(data) {
            switch (data.type) {
                case 'new':
                    this.handleNewMessage(data);
                    break;
                case 'edit':
                    this.handleEditMessage(data);
                    break;
                case 'delete':
                    this.handleDeleteMessage(data);
                    break;
                case 'media_ready':
                    this.handleMediaReady(data);
                    break;
                case 'media_status':
                    this.handleMediaStatus(data);
                    break;
            }
        },
        handleMediaStatus(data) {
            console.log(`Media status: msg=${data.message_id}, status=${data.status}, progress=${data.progress}%`);
            MediaManager.handleMediaStatus(
                data.message_id,
                data.status,
                data.progress,
                data.media_id
            );
        },
        handleEventBatch(batch) {
            console.log(`Processing batch of ${batch.events.length} events, is_replay: ${batch.is_replay}`);
            const edits = [];
            const news = [];
            const deletes = [];
            const mediaReadies = [];
            const mediaStatuses = [];
            batch.events.forEach(event => {
                if (event.channel_id !== parseInt(CONFIG.CHANNEL_ID)) return;
                if (event.version && !State.supportedVersions.includes(event.version)) return;
                const messageKey = `${event.channel_id}-${event.message_id}-${event.type}`;
                if (State.batchProcessed.has(messageKey)) return;
                State.batchProcessed.add(messageKey);
                switch (event.type) {
                    case 'edit':
                        edits.push(event);
                        break;
                    case 'new':
                        news.push(event);
                        break;
                    case 'delete':
                        deletes.push(event);
                        break;
                    case 'media_ready':
                        mediaReadies.push(event);
                        break;
                    case 'media_status':
                        mediaStatuses.push(event);
                        break;
                }
            });
            safeSetTimeout(() => {
                State.batchProcessed.clear();
            }, 100);
            if (edits.length > 0) {
                this.handleBatchEdits(edits, batch.is_replay);
            }
            if (news.length > 0) {
                this.handleBatchNews(news);
            }
            deletes.forEach(event => this.handleDeleteMessage(event));
            mediaReadies.forEach(event => this.handleMediaReady(event));
            mediaStatuses.forEach(event => this.handleMediaStatus(event));
        },
        async handleBatchEdits(editEvents, isReplay = false) {
            console.log(`Processing ${editEvents.length} edit events in batch`);
            const messageIds = editEvents.map(e => e.message_id);
            messageIds.forEach(id => MessageAPI.invalidateMessage(id));
            const messages = await MessageAPI.fetchBatchMessages(messageIds);
            let updatedCount = 0;
            Object.entries(messages).forEach(([id, fullMessage]) => {
                const messageId = parseInt(id);
                const post = State.posts.get(messageId);
                if (!post) return;
                const hasChanges =
                    (fullMessage.text !== undefined && post.text !== fullMessage.text) ||
                    (fullMessage.views !== undefined && post.views !== fullMessage.views) ||
                    (fullMessage.media?.url && post.media_url !== fullMessage.media.url) ||
                    (fullMessage.edit_date && post.edit_date !== fullMessage.edit_date);
                if (hasChanges) {
                    UI.updatePostQuiet(messageId, this.normalizePostData(fullMessage));
                    updatedCount++;
                }
            });
            if (updatedCount > 0) {
                console.log(`Updated ${updatedCount} posts in batch`);
            }
        },
        handleBatchNews(newEvents) {
            const messageIds = newEvents.map(e => e.message_id);
            MessageAPI.fetchBatchMessages(messageIds).then(messages => {
                const posts = Object.values(messages);
                posts.sort((a, b) => b.message_id - a.message_id);
                let addedCount = 0;
                posts.forEach(post => {
                    const normalizedPost = this.normalizePostData(post);
                    if (!State.posts.has(post.message_id) &&
                        !State.newPosts.some(p => p.message_id === post.message_id)) {
                        if (window.scrollY < 400) {
                            UI.addPostToTop(normalizedPost);
                            addedCount++;
                        } else {
                            State.newPosts.push(normalizedPost);
                        }
                    }
                });
                if (addedCount > 0) {
                    console.log(`Added ${addedCount} new posts from batch`);
                }
                UI.updateNewPostsBadge();
            });
        },
        normalizePostData(fullMessage) {
            let displayMediaType = fullMessage.media?.file_type || fullMessage.media_type;
            if (displayMediaType === 'webpage_preview' && fullMessage.media?.url) {
                displayMediaType = 'photo';
            }
            return {
                message_id: fullMessage.message_id,
                text: fullMessage.text || '',
                date: fullMessage.date,
                views: fullMessage.views || 0,
                forwards: fullMessage.forwards || 0,
                has_media: fullMessage.has_media || !!fullMessage.media,
                media_type: displayMediaType,
                media_url: fullMessage.media?.url || fullMessage.media_url,
                media_pending: fullMessage.media && !fullMessage.media.uploaded,
                is_edited: fullMessage.is_edited || false,
                edit_date: fullMessage.edit_date
            };
        },
        async handleNewMessage(data) {
            if (data.data) {
                this.processFullMessage(data.data, 'new');
                return;
            }
            const fullMessage = await MessageAPI.fetchFullMessage(data.message_id);
            if (fullMessage) {
                this.processFullMessage(fullMessage, 'new');
            }
        },
        async handleEditMessage(data) {
            if (data.data) {
                this.processFullMessage(data.data, 'edit');
                return;
            }
            MessageAPI.invalidateMessage(data.message_id);
            const fullMessage = await MessageAPI.fetchFullMessage(data.message_id);
            if (fullMessage) {
                this.processFullMessage(fullMessage, 'edit');
            }
        },
        handleMediaReady(data) {
            const messageId = data.message_id;
            const mediaUrl = data.media_url;
            const mediaType = data.media_type;
            MediaManager.handleMediaReady(messageId, mediaUrl, mediaType);
        },
        handleDeleteMessage(data) {
            State.posts.delete(data.message_id);
            const index = State.postOrder.indexOf(data.message_id);
            if (index !== -1) State.postOrder.splice(index, 1);
            UI.deletePost(data.message_id);
            State.mediaPending.delete(data.message_id);
            State.mediaLoading.delete(data.message_id);
            State.mediaRetryCount.delete(data.message_id);
            State.mediaStatusStore.delete(data.message_id);
            State.mediaReadyQueue.delete(data.message_id);
            State.fullMessageCache.delete(data.message_id);
            State.failedMedia.delete(data.message_id);
            const queueIndex = State.loadQueue.indexOf(Number(data.message_id));
            if (queueIndex > -1) {
                State.loadQueue.splice(queueIndex, 1);
            }
        },
        processFullMessage(fullMessage, type = 'new') {
            const messageId = fullMessage.message_id;
            const actualType = type === 'edit' ? 'edit' : 'new';
            const post = this.normalizePostData(fullMessage);
            const existingPost = State.posts.get(messageId);
            const existingInQueue = State.newPosts.some(p => p.message_id === messageId);
            const existingInDOM = document.querySelector(`.post[data-message-id="${messageId}"]`);
            if (actualType === 'edit') {
                if (existingInDOM) {
                    if (existingPost) {
                        const hasChanges =
                            post.text !== existingPost.text ||
                            post.views !== existingPost.views ||
                            post.media_url !== existingPost.media_url;
                        if (hasChanges) {
                            UI.updatePost(messageId, post);
                            State.posts.set(messageId, {...post});
                        }
                    } else {
                        UI.updatePost(messageId, post);
                    }
                } else if (existingInQueue) {
                    const index = State.newPosts.findIndex(p => p.message_id === messageId);
                    if (index !== -1) {
                        State.newPosts[index] = {...post};
                    }
                } else if (existingPost) {
                    State.posts.set(messageId, {...post});
                }
            } else {
                if (existingInDOM || existingPost || existingInQueue) {
                    return;
                }
                if (window.scrollY < 400) {
                    UI.addPostToTop(post);
                } else {
                    State.newPosts.push(post);
                    UI.updateNewPostsBadge();
                }
            }
            if (post.has_media && !post.media_url) {
                safeSetTimeout(() => {
                    MediaManager.loadMedia(messageId);
                }, 500);
            }
        },
        flushNewPosts() {
            if (State.newPosts.length === 0) return;
            const postsToFlush = [...State.newPosts].sort((a, b) => b.message_id - a.message_id);
            State.newPosts = [];
            UI.updateNewPostsBadge();
            let addedCount = 0;
            postsToFlush.forEach(post => {
                const inDOM = document.querySelector(`.post[data-message-id="${post.message_id}"]`);
                if (inDOM) return;
                UI.addPostToTop(post);
                addedCount++;
            });
        },
        reconnect() {
            if (this.giveUp || State.wsReconnectAttempts >= CONFIG.MAX_RECONNECT_ATTEMPTS) {
                return;
            }
            State.wsReconnectAttempts++;
            const baseDelay = Math.min(
                CONFIG.RECONNECT_BASE_DELAY * Math.pow(1.5, State.wsReconnectAttempts),
                30000
            );
            const jitterRange = baseDelay * 0.3;
            const jitter = (Math.random() * jitterRange * 2) - jitterRange;
            const delay = Math.max(1000, baseDelay + jitter);
            safeSetTimeout(() => {
                if (!State.wsConnected && !this.giveUp) {
                    const lastEventId = State.lastEventId || 0;
                    const wsUrl = lastEventId > 0
                        ? `${CONFIG.WS_BASE}?last_event_id=${lastEventId}`
                        : CONFIG.WS_BASE;
                    this.connect(wsUrl);
                }
            }, delay);
        }
    };

    const ScrollHandler = {
        init() {
            State.lastDocumentHeight = document.documentElement.scrollHeight;
            State.resizeObserver = new ResizeObserver(() => {
                State.lastDocumentHeight = document.documentElement.scrollHeight;
            });
            State.resizeObserver.observe(document.documentElement);
            window.addEventListener('scroll', this.throttledHandle.bind(this), { passive: true });
        },
        handle(scrollY) {
            UI.showScrollTopButton(scrollY > 500);
            if (scrollY + window.innerHeight >= State.lastDocumentHeight - 500) {
                this.debouncedLoadMore();
            }
            if (scrollY < 200 && State.newPosts.length > 0) {
                WebSocketManager.flushNewPosts();
            }
        },
        throttledHandle: (() => {
            let ticking = false;
            let lastScrollY = 0;
            return function() {
                lastScrollY = window.scrollY;
                if (!ticking) {
                    requestAnimationFrame(() => {
                        this.handle(lastScrollY);
                        ticking = false;
                    });
                    ticking = true;
                }
            };
        })(),
        debouncedLoadMore: debounce(() => {
            if (!State.isLoading && State.hasMore) {
                MessageLoader.loadMessages();
            }
        }, 300, { leading: true, trailing: false })
    };

    function init() {
        window.__videoManager = VideoManager;
        ThemeManager.init();
        UI.updateChannelInfo();
        CacheManager.startCleanupInterval();
        if (!UI.restoreDOM()) {
            loadInitialAndProcessPending();
        }
        WebSocketManager.connect();
        ScrollHandler.init();

        document.getElementById('feed').addEventListener('click', (e) => {
            const container = e.target.closest('.media-container');
            if (container) {
                const post = container.closest('.post');
                if (post && post.dataset.mediaUrl) {
                    Lightbox.open(post.dataset.mediaUrl, post.dataset.mediaType);
                    e.preventDefault();
                    e.stopPropagation();
                }
            }
        });

        document.getElementById('channelAvatar').addEventListener('click', () => ThemeManager.toggle());
        document.getElementById('newPostsBadge').addEventListener('click', () => {
            WebSocketManager.flushNewPosts();
            window.scrollTo({ top: 0, behavior: 'smooth' });
        });
        document.getElementById('scrollTopBtn').addEventListener('click', () => {
            window.scrollTo({ top: 0, behavior: 'smooth' });
        });
        document.getElementById('lightboxClose').addEventListener('click', Lightbox.close);
        document.getElementById('lightbox').addEventListener('click', (e) => {
            if (e.target === document.getElementById('lightbox')) Lightbox.close();
        });

        window.addEventListener('online', () => {
            console.log('Network is online');
            UI.handleNetworkOnline();
        });
        window.addEventListener('offline', () => {
            console.log('Network is offline');
        });
        document.addEventListener('visibilitychange', () => {
            if (!document.hidden && State.newPosts.length > 0) {
                WebSocketManager.flushNewPosts();
            }
        });
        window.addEventListener('beforeunload', () => {
            UI.cacheDOM();
            cleanupResources();
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
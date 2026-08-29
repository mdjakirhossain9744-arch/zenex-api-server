import Fastify from 'fastify';
import mongoose from 'mongoose';
import dotenv from 'dotenv';
import { User, Order } from './models.js';
import fastifyFormbody from '@fastify/formbody'; 
import fastifyCors from '@fastify/cors'; 
import Redis from "ioredis"; 
import fastifyCompress from '@fastify/compress'; 

dotenv.config();

const fastify = Fastify({ logger: false, trustProxy: true });
const redis = new Redis(); 

fastify.register(fastifyCors, { origin: '*', methods: ['GET', 'POST', 'OPTIONS'], allowedHeaders: ['Content-Type', 'mapikey'] });
fastify.register(fastifyFormbody); 
fastify.register(fastifyCompress, { global: true, encodings: ['br', 'gzip', 'deflate'] });

const connectDB = async () => {
    try {
        await mongoose.connect(process.env.MONGODB_URI, { maxPoolSize: 150, minPoolSize: 10 });
        console.log(`✅ ZENEX Database Connected! 🚀`);
    } catch (error) { process.exit(1); }
};

const getUTCDateString = (dateObj = new Date()) => new Date(dateObj).toISOString().split('T')[0];

const IPRN_API_URL = "https://api.iprn-elite.com/v1.0";
const IPRN_API_KEY = process.env.IPRN_API_KEY || "1ddOYcGxRcWUlyi6T7oZzA"; 

const globalSdeMap = new Map();
const fetchSdeList = async () => {
    try {
        const payload = { jsonrpc: "2.0", method: "sms.realtime:get_subdestination_list", params: {}, id: Date.now() };
        const res = await fetch(IPRN_API_URL, { method: "POST", headers: { "Api-Key": IPRN_API_KEY, "Content-Type": "application/json" }, body: JSON.stringify(payload) });
        const data = await res.json();
        if (data?.result?.subdestination_list) {
            data.result.subdestination_list.forEach(item => { globalSdeMap.set(item.sde_key, item.name); });
        }
    } catch (e) {}
};

const apiAuthCache = new Map();
const globalWorkerUserCache = new Map(); 
const userOtpResponseCache = new Map(); 

let cachedMaskingSettings = { keywords: [], expiry: 0 };
async function getMaskingKeywords() {
    if (Date.now() < cachedMaskingSettings.expiry) return cachedMaskingSettings.keywords;
    try {
        const db = mongoose.connection.db;
        const settings = await db.collection("system_settings").findOne({ type: "global" });
        const kw = settings?.hiddenKeywords || [];
        cachedMaskingSettings = { keywords: kw, expiry: Date.now() + 60000 }; return kw;
    } catch (e) { return cachedMaskingSettings.keywords; }
}

const extractStrictOTP = (rawText) => {
    if (!rawText) return "00000";
    const match = rawText.match(/\d{3}[\s-]\d{3,4}|\d{4,8}/);
    return match ? match[0].trim() : "00000";
};

const escapeRegExp = (string) => string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

const applyMasking = (text, keywords) => {
    if (!text) return text;
    let masked = text;
    keywords.forEach(w => {
        const word = w.trim();
        if (word && word.length > 1) {
            const regex = new RegExp(escapeRegExp(word), 'gi');
            masked = masked.replace(regex, (match) => match.replace(/[^\s]/g, '*'));
        }
    });
    return masked;
};

setInterval(() => {
    const now = Date.now();
    for (const [key, value] of apiAuthCache.entries()) { if (now > value.expiry) apiAuthCache.delete(key); }
    for (const [key, value] of userOtpResponseCache.entries()) { if (now > value.expiry) userOtpResponseCache.delete(key); }
}, 30000); 

setInterval(() => { globalWorkerUserCache.clear(); }, 5 * 60 * 1000); 

async function triggerBinanceAutoPay(user) {
    try {
        const res = await fetch(`${process.env.MAIN_SITE_URL}/api/cron/process-binance-payout`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ userId: user._id })
        });
        const result = await res.json().catch(() => ({}));
        if (result && result.success === false) {
            await User.findOneAndUpdate({ _id: user._id }, { $set: { autoPayEnabled: false } }, { returnDocument: 'after' });
            if (globalWorkerUserCache.has(user.email)) {
                let cachedUser = globalWorkerUserCache.get(user.email);
                cachedUser.autoPayEnabled = false;
                globalWorkerUserCache.set(user.email, cachedUser);
            }
        }
    } catch (e) {}
}

const extractServiceName = (msg) => {
    if (!msg) return "Other";
    const text = msg.toLowerCase();
    if (text.includes('facebook') || text.includes(' fb ') || text.includes('facebk') || text.includes('fb.me') || text.includes('ফেসবুক') || text.includes('ফেচবুক')) return 'Facebook';
    if (text.includes('whatsapp') || text.includes(' wa ') || text.includes('vwaq') || text.includes('wa.me')) return 'WhatsApp';
    if (text.includes('telegram') || text.includes('t.me')) return 'Telegram';
    if (text.includes('instagram') || text.includes(' ig ') || text.includes('ig.me')) return 'Instagram';
    if (text.includes('google') || /g-\d+/.test(text) || text.includes('gmail') || text.includes('youtube')) return 'Google';
    if (text.includes('imo')) return 'IMO';
    if (text.includes('viber')) return 'Viber';
    if (text.includes('meta')) return 'Meta';
    if (text.includes('tiktok') || text.includes(' tt ')) return 'TikTok';
    if (text.includes('snapchat')) return 'Snapchat';
    if (text.includes('twitter') || text.includes(' x ')) return 'X';
    return "Other"; 
};

fastify.route({
    method: ['GET', 'POST'], 
    url: '/v1/getnum',
    handler: async (request, reply) => {
        try {
            const apiKey = request.headers['mapikey'] || (request.query && request.query.mapikey);
            if (!apiKey || apiKey.trim().length < 10) return reply.status(401).send({ meta: { status: "error" }, message: "Invalid API Key" });

            const cleanKey = apiKey.trim();
            let cachedObj = apiAuthCache.get(cleanKey);
            let user;

            if (!cachedObj || Date.now() > cachedObj.expiry) {
                user = await User.findOne({ apiKey: cleanKey }).lean();
                if (!user || !user.isApiActive || user.status !== "active") return reply.status(403).send({ meta: { status: "error" }, message: "Unauthorized" });
                apiAuthCache.set(cleanKey, { user, expiry: Date.now() + 60000 });
            } else { user = cachedObj.user; }

            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 10000); 
            request.raw.on('close', () => { if (request.raw.aborted) controller.abort(); });

            const reqData = request.body || request.query || {};
            const rawRange = typeof reqData === 'string' ? reqData : (reqData.range || "");
            const rid = rawRange.replace(/x/gi, '').trim();

            if (!rid) { clearTimeout(timeoutId); return reply.status(400).send({ meta: { status: "error" }, message: "Invalid Range Format" }); }

            let response;
            try {
                const payload = { jsonrpc: "2.0", method: "sms.realtime:allocate", params: { senderid: "OTP", prefix_list: [String(rid).toUpperCase().replace(/X/g, '')], dont_check_access: true }, id: Date.now() };
                response = await fetch(IPRN_API_URL, { method: "POST", headers: { "Api-Key": IPRN_API_KEY, "Content-Type": "application/json" }, body: JSON.stringify(payload), signal: controller.signal });
                clearTimeout(timeoutId);
            } catch (fetchError) {
                clearTimeout(timeoutId); return reply.status(504).send({ meta: { status: "error" }, message: "Provider is slow. Try again." });
            }

            let data;
            try { data = await response.json(); } catch(e) { return reply.status(502).send({ meta: { status: "error" }, message: "Invalid upstream response" }); }

            if (data.result && data.result.number && data.result.number.full) {
                const trxId = data.result.message_id || "";
                const fullNumStr = String(data.result.number.full || "");
                
                let exactCountry = "Unknown"; let exactOperator = "Mobile"; 
                if (data.result.sde_key && globalSdeMap.has(data.result.sde_key)) {
                    let rawName = globalSdeMap.get(data.result.sde_key);
                    rawName = rawName.replace(/\s*\([\d+X]+\)\s*$/g, '').trim();
                    const parts = rawName.split(' - ');
                    exactCountry = parts[0] ? parts[0].trim() : "Unknown";
                    if (parts.length >= 3) { exactOperator = parts[2].trim(); } 
                    else if (parts.length === 2) { exactOperator = parts[1].trim().toLowerCase() === "mobile" ? "Mobile" : parts[1].trim(); }
                }

                const todayStr = getUTCDateString();
                setImmediate(() => {
                    const newOrder = new Order({
                        userEmail: user.email, searchNumber: fullNumStr, displayNumber: `+${fullNumStr}`, requestedRange: rid,
                        trxId: String(trxId), country: exactCountry, operator: exactOperator, status: "WAIT",
                        fullMessage: "Waiting...", otp: "Waiting...", dateString: todayStr, expireAt: new Date(Date.now() + 2 * 24 * 60 * 60 * 1000)
                    });
                    newOrder.save().catch(() => {});
                });
                
                return reply.status(200).send({
                    meta: { status: "success", code: 200 },
                    data: { copy: `+${fullNumStr}`, number: `+${fullNumStr}`, full_number: fullNumStr, country: exactCountry, iso: "Unknown", operator: exactOperator, status: "pending" }
                });
            }
            return reply.status(400).send({ meta: { status: "error" }, message: data.error?.message || "Out of stock or Invalid Range" });
        } catch (error) { return reply.status(500).send({ meta: { status: "error" }, message: "Server Error" }); }
    }
});

const processIncomingOTP = async (trunkTxId, rawText, senderId, destNum, smsId) => {
    if (senderId && senderId !== "Unknown") {
        console.log(`🔥 [OFFICIAL SENDER ID DETECTED]: ${senderId}`);
    }
    
    if (!rawText) return;

    const uniqueKey = (smsId && smsId !== "no_id") ? smsId : trunkTxId;
    if (uniqueKey) {
        const lockAcquired = await redis.set(`iprn_sms_${uniqueKey}`, "locked", "NX", "EX", 86400); 
        if (!lockAcquired) return; 
    }
    
    let text = rawText.replace(/[<#>]/g, '').replace(/\n/g, ' ').replace(/\r/g, '').replace(/\s{2,}/g, ' ').trim();
    const cleanDestNum = String(destNum).replace('+', '');
    
    const query = { $or: [] };
    if (cleanDestNum) query.$or.push({ searchNumber: cleanDestNum }, { displayNumber: `+${cleanDestNum}` });
    
    if (query.$or.length === 0) return;

    const existingOrders = await Order.find(query).sort({ _id: -1 }).limit(5); 
    if (existingOrders.length === 0) return;

    let baseOrder = existingOrders.find(o => o.status === "WAIT");
    if (!baseOrder) baseOrder = existingOrders[0]; 

    const orderAgeInMs = Date.now() - new Date(baseOrder.createdAt).getTime();
    if (orderAgeInMs > 25 * 60 * 1000 || baseOrder.status === "FAIL" || baseOrder.status === "CANCEL") return;
    
    const strictOtp = extractStrictOTP(text);

    let userEarned = 0; let agentEarned = 0;
    try {
        const actualUser = await User.findOne({ email: baseOrder.userEmail }).lean();
        if (actualUser) {
            // 💥 BOSS UPDATE: All services (including WhatsApp/Telegram) now receive normal pay rate 💥
            let rawOtpCost = Number(actualUser.otpRate) || 0;
            userEarned = Math.abs(rawOtpCost);
            
            let actualAgent = null;
            if (actualUser.agentEmail && actualUser.agentEmail !== "admin") {
                actualAgent = await User.findOne({ $or: [ { email: actualUser.agentEmail }, { customAgentMail: actualUser.agentEmail } ], role: "agent" }).lean();
                if (actualAgent) {
                    const aRate = Number(actualAgent.agentMaxRate || 0.70);
                    const profit = Math.max(0, Number((aRate - userEarned).toFixed(4)));
                    if (profit > 0) agentEarned = profit;
                }
            }

            if (userEarned > 0) {
                const updatedUser = await User.findOneAndUpdate({ _id: actualUser._id }, { $inc: { balance: userEarned } }, { returnDocument: 'after' });
                if (updatedUser && (updatedUser.autoPayEnabled === true || updatedUser.autoPayEnabled === "true") && updatedUser.balance >= 150) { triggerBinanceAutoPay(updatedUser).catch(() => {}); }
            }
            if (agentEarned > 0 && actualAgent) { await User.updateOne({ _id: actualAgent._id }, { $inc: { balance: agentEarned, agentEarning: agentEarned } }); }
        }
    } catch (balanceErr) {}

    let detectedService = extractServiceName(text);
    let finalTrueService = (senderId && senderId !== "Unknown") ? senderId : (detectedService !== "Other" ? detectedService : "Other");

    if (baseOrder.status === "WAIT") {
        baseOrder.status = "DONE"; baseOrder.otp = strictOtp; baseOrder.fullMessage = text; 
        baseOrder.trueService = finalTrueService; baseOrder.orderCost = userEarned; baseOrder.orderCommission = agentEarned; 
        await baseOrder.save();
        console.log(`✅ [DELIVERED] ${cleanDestNum} | App: ${finalTrueService} | OTP: ${strictOtp}`);
    } else {
        const newMultiOrder = new Order({
            userEmail: baseOrder.userEmail, userName: baseOrder.userName, userUid: baseOrder.userUid, agentEmail: baseOrder.agentEmail,
            searchNumber: baseOrder.searchNumber, displayNumber: baseOrder.displayNumber, country: baseOrder.country, operator: baseOrder.operator,
            dateString: baseOrder.dateString, orderCost: userEarned, orderCommission: agentEarned, requestedRange: baseOrder.requestedRange,
            trxId: baseOrder.trxId, status: "DONE", otp: strictOtp, fullMessage: text, trueService: finalTrueService, expireAt: new Date(Date.now() + 2 * 24 * 60 * 60 * 1000)
        });
        await newMultiOrder.save();
        console.log(`✅ [MULTI-DELIVERED] ${cleanDestNum} | App: ${finalTrueService} | OTP: ${strictOtp}`);
    }
};

let isPollingIPRN = false;
const pollIPRNPendingOrders = async () => {
    if (isPollingIPRN) return;
    const lockAcquired = await redis.set("iprn_poll_lock", "locked", "NX", "EX", 3);
    if (!lockAcquired) return; 

    isPollingIPRN = true;
    try {
        const payload = { jsonrpc: "2.0", method: "sms.mdr_full:get_list", params: { limit: 500 }, id: Date.now() };
        const res = await fetch(IPRN_API_URL, { method: "POST", headers: { "Api-Key": IPRN_API_KEY, "Content-Type": "application/json" }, body: JSON.stringify(payload) });
        const data = await res.json();
        const messages = data?.result?.mdr_full_list || data?.result?.mdr_list || [];
        
        if (messages.length > 0) {
            for (const msg of messages) {
                const trunkTxId = msg.message_id || msg.trunk_number_transaction_id || "";
                const text = msg.message || msg.text || msg.content || "";
                const senderId = msg.senderid || msg.source_addr || "Unknown";
                const destNum = msg.phone || msg.destination_addr || msg.number || "";
                if (text && destNum) await processIncomingOTP(trunkTxId, text, senderId, destNum, "no_id");
            }
        }

        const fifteenMinsAgo = new Date(Date.now() - 15 * 60 * 1000); 
        const pendingOrders = await Order.find({ status: "WAIT", trxId: { $ne: "", $exists: true }, createdAt: { $gte: fifteenMinsAgo } }).sort({ _id: -1 }).limit(300).lean();

        if (pendingOrders.length > 0) {
            const chunkSize = 10; 
            for (let i = 0; i < pendingOrders.length; i += chunkSize) {
                const chunk = pendingOrders.slice(i, i + chunkSize);
                await Promise.allSettled(chunk.map(async (order) => {
                    try {
                        const fallPayload = { jsonrpc: "2.0", method: "sms.realtime:get_message", params: { message_id: order.trxId }, id: Date.now() };
                        const fallRes = await fetch(IPRN_API_URL, { method: "POST", headers: { "Api-Key": IPRN_API_KEY, "Content-Type": "application/json" }, body: JSON.stringify(fallPayload) });
                        const fallData = await fallRes.json();
                        if (fallData?.result?.reply === "success" && fallData.result.message) {
                            await processIncomingOTP(order.trxId, fallData.result.message, fallData.result.senderid || "Unknown", order.searchNumber, "no_id");
                        }
                    } catch(e) {}
                }));
                await new Promise(r => setTimeout(r, 150));
            }
        }
    } catch (error) {
        console.error("Unstoppable Engine Error:", error.message);
    } finally {
        isPollingIPRN = false;
    }
};
setInterval(pollIPRNPendingOrders, 4000);

fastify.route({
    method: ['GET', 'POST'],
    url: '/v1/webhook/iprn-receive',
    handler: async (request, reply) => {
        try {
            const reqIp = request.headers['cf-connecting-ip'] || request.headers['x-forwarded-for'] || request.ip;
            const data = { ...(request.query || {}), ...(request.body || {}) };
            
            console.log(`\n=========================================`);
            console.log(`🔥 [WEBHOOK RAW HIT] Method: ${request.method} | IP: ${reqIp}`);
            console.log(`📦 [PAYLOAD]: ${JSON.stringify(data)}`);
            console.log(`=========================================\n`);

            const trunkTxId = data.smsid || data.message_id || data.trunk_number_transaction_id || data.trxId;
            const text = data.message || data.smstext || data.text || data.content;
            const senderId = data.from || data.senderid || data.source_addr || "Unknown";
            const destNum = data.to || data.called_number || data.destination_addr || data.number || data.b_number;
            const smsId = data.smsid || data.smsid2 || "no_id";
            
            if (!text) return reply.status(400).send({ success: false, message: "No text found in payload" });
            processIncomingOTP(trunkTxId, text, senderId, destNum, smsId).catch(console.error);
            return reply.status(200).send({ success: true, message: "Webhook processed" });
        } catch (error) { return reply.status(500).send({ success: false, message: "Internal Error" }); }
    }
});

fastify.get('/v1/numsuccess/info', async (request, reply) => {
    try {
        const apiKey = request.headers['mapikey'];
        if (!apiKey || apiKey.trim().length < 10) return reply.status(401).send({ meta: { status: "error" }, message: "Missing API Key" });
        const cleanKey = apiKey.trim();

        const cachedOtpData = userOtpResponseCache.get(cleanKey);
        if (cachedOtpData && Date.now() < cachedOtpData.expiry) return reply.status(200).send({ meta: { status: "success", code: 200 }, data: { otps: cachedOtpData.otps } });

        let cachedObj = apiAuthCache.get(cleanKey); let user;
        if (!cachedObj || Date.now() > cachedObj.expiry) {
            user = await User.findOne({ apiKey: cleanKey }).select("email isApiActive").lean();
            if (user) apiAuthCache.set(cleanKey, { user, expiry: Date.now() + 60000 });
        } else { user = cachedObj.user; }

        if (!user || !user.isApiActive) return reply.status(401).send({ meta: { status: "error" }, message: "Unauthorized" });

        const hiddenKeywords = await getMaskingKeywords();
        const recentOrders = await Order.find({ userEmail: user.email, status: "DONE", updatedAt: { $gte: new Date(Date.now() - 20 * 60 * 1000) } })
            .select("_id displayNumber searchNumber otp fullMessage country operator updatedAt createdAt status").sort({ updatedAt: -1 }).lean();

        let expandedOtps = [];
        recentOrders.forEach(order => {
            const d = new Date(order.updatedAt || order.createdAt);
            const pad = (n) => n.toString().padStart(2, '0');
            const formattedDate = `${d.getUTCFullYear()}-${pad(d.getUTCMonth()+1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
            const numberClean = String(order.displayNumber || order.searchNumber || "").replace(/\D/g, "");
            const baseNid = "ZX_" + order._id.toString().substring(0, 10).toUpperCase();

            let rawMsg = order.fullMessage || order.otp || "";
            if (rawMsg.includes("_||_")) {
                rawMsg.split("_||_").map(m => m.trim()).filter(Boolean).forEach((msg, idx) => {
                    expandedOtps.push({ nid: `${baseNid}_${idx}`, number: numberClean, otp: applyMasking(msg, hiddenKeywords), country: order.country || "Unknown", operator: order.operator || "Any", created_at: formattedDate });
                });
            } else { expandedOtps.push({ nid: `${baseNid}_0`, number: numberClean, otp: applyMasking(rawMsg, hiddenKeywords), country: order.country || "Unknown", operator: order.operator || "Any", created_at: formattedDate }); }
        });

        const validOtps = expandedOtps.filter(o => o.otp && o.otp.trim() !== "" && !["waiting...", "pending", "null"].includes(o.otp.toLowerCase()));
        userOtpResponseCache.set(cleanKey, { otps: validOtps, expiry: Date.now() + 1500 });
        return reply.status(200).send({ meta: { status: "success", code: 200 }, data: { otps: validOtps } });
    } catch (error) { return reply.status(500).send({ meta: { status: "error" } }); }
});

let cachedActiveData = null;
let lastFetchTime = 0;
const CACHE_DURATION = 60 * 1000; 

fastify.get('/v1/active-ranges', async (request, reply) => {
    try {
        const apiKey = request.headers['mapikey'];
        if (!apiKey || apiKey.trim().length < 10) return reply.status(401).send({ success: false, message: "Invalid API Key" });

        if (cachedActiveData && (Date.now() - lastFetchTime < CACHE_DURATION)) {
            return reply.send({ success: true, cached: true, message: "Global routing ranges fetched", data: cachedActiveData });
        }

        const hiddenKeywords = await getMaskingKeywords();
        const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
        
        const recentOrders = await Order.find({ 
            status: { $in: ["DONE", "Success", "SUCCESS"] }, 
            updatedAt: { $gte: oneHourAgo } 
        }).select("fullMessage otp searchNumber number trueService").lean();
        
        const rangeMap = {};

        recentOrders.forEach((o) => {
            let msg = o.fullMessage || o.otp || "";
            let rawService = (o.trueService && o.trueService !== "Unknown" && o.trueService !== "Other") 
                ? String(o.trueService) 
                : extractServiceName(msg);
                
            const maskedService = applyMasking(rawService, hiddenKeywords); 

            let num = o.searchNumber || o.number || "";
            num = String(num).replace("+", "");
            
            if (num.length >= 6) {
                const rangeStr = num.substring(0, 6) + "XXX"; 
                let tag = "General";
                
                if (rawService.toLowerCase().includes("facebook") || rawService.toLowerCase().includes("meta")) {
                    const match = msg.match(/\b\d{4,8}\b/);
                    if (match) {
                        if (match[0].length === 6 || match[0].length === 8) tag = "Fb Clone";
                        else if (match[0].length === 5) tag = "New Fb";
                    }
                }
                
                const maskedTag = applyMasking(tag, hiddenKeywords); 

                const key = `${rangeStr}|${maskedService}|${maskedTag}`;
                if (!rangeMap[key]) rangeMap[key] = { range: rangeStr, service: maskedService, tag: maskedTag, hits: 0 };
                rangeMap[key].hits += 1;
            }
        });

        const formattedRanges = Object.values(rangeMap).sort((a, b) => b.hits - a.hits).slice(0, 10);
        cachedActiveData = { active_ranges: formattedRanges };
        lastFetchTime = Date.now();

        return reply.send({ success: true, cached: false, message: "Global routing ranges fetched", data: cachedActiveData });
    } catch (error) { 
        return reply.status(500).send({ success: false, message: "Server Error", data: { active_ranges: [] } }); 
    }
});

fastify.get('/v1/user/today-otps', async (request, reply) => reply.type('text/plain').send("NO_DATA"));

const startServer = async () => {
    try {
        await connectDB();
        await fetchSdeList(); 
        await fastify.listen({ port: process.env.PORT || 4000, host: '0.0.0.0' });
        console.log(`⚡ ZENEX Microservice V7 (Silent Poller + Multi-OTP Guard + Matrix) is LIVE!`);
    } catch (err) { process.exit(1); }
};
startServer();
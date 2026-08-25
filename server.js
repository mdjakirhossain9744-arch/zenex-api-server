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

fastify.register(fastifyCors, { 
    origin: '*',
    methods: ['GET', 'POST', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'mapikey']
});

fastify.register(fastifyFormbody); 

fastify.register(fastifyCompress, {
    global: true,
    encodings: ['br', 'gzip', 'deflate'] 
});

const connectDB = async () => {
    try {
        const opts = { maxPoolSize: 150, minPoolSize: 10 };
        await mongoose.connect(process.env.MONGODB_URI, opts);
        console.log(`✅ ZENEX Database Connected to API Microservice [Instance: ${process.pid}]! 🚀`);
    } catch (error) {
        console.error('❌ Database Connection Failed:', error);
        process.exit(1);
    }
};

const getUTCDateString = (dateObj = new Date()) => new Date(dateObj).toISOString().split('T')[0];

// 💥 IPRN ELITE INTEGRATION 💥
const IPRN_API_URL = "https://api.iprn-elite.com/v1.0";
const IPRN_API_KEY = process.env.IPRN_API_KEY || "1ddOYcGxRcWUlyi6T7oZzA"; 

const globalSdeMap = new Map();

const fetchSdeList = async () => {
    try {
        const payload = { 
            jsonrpc: "2.0", 
            method: "sms.realtime:get_subdestination_list", 
            params: {}, 
            id: Date.now() 
        };
        const res = await fetch(IPRN_API_URL, { 
            method: "POST", 
            headers: { "Api-Key": IPRN_API_KEY, "Content-Type": "application/json" }, 
            body: JSON.stringify(payload) 
        });
        const data = await res.json();

        if (data?.result?.subdestination_list) {
            data.result.subdestination_list.forEach(item => { 
                globalSdeMap.set(item.sde_key, item.name); 
            });
            console.log(`✅ IPRN SDE Dictionary Loaded: ${globalSdeMap.size} destinations cached.`);
        }
    } catch (e) {
        console.error("⚠️ Failed to load SDE list:", e.message);
    }
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
        cachedMaskingSettings = { keywords: kw, expiry: Date.now() + 60000 }; 
        return kw;
    } catch (e) {
        return cachedMaskingSettings.keywords;
    }
}

const extractStrictOTP = (rawText) => {
    if (!rawText) return "00000";
    const match = rawText.match(/(?:\b\d{4,8}\b)|(?:\b\d{3}[\s-]\d{3,4}\b)/);
    return match ? match[0].trim() : "00000";
};

const escapeRegExp = (string) => {
    return string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
};

const applyMasking = (text, keywords) => {
    if (!text) return text;
    let masked = text;
    keywords.forEach(w => {
        const word = w.trim();
        if (word && word.length > 1) {
            const regex = new RegExp(escapeRegExp(word), 'gi');
            masked = masked.replace(regex, (match) => {
                return match.replace(/[^\s]/g, '*');
            });
        }
    });
    return masked;
};

setInterval(() => {
    const now = Date.now();
    for (const [key, value] of apiAuthCache.entries()) {
        if (now > value.expiry) apiAuthCache.delete(key);
    }
    for (const [key, value] of userOtpResponseCache.entries()) {
        if (now > value.expiry) userOtpResponseCache.delete(key);
    }
}, 30000); 

setInterval(() => { globalWorkerUserCache.clear(); }, 5 * 60 * 1000); 

async function triggerBinanceAutoPay(user) {
    try {
        const res = await fetch(`${process.env.MAIN_SITE_URL}/api/cron/process-binance-payout`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ userId: user._id })
        });
        const result = await res.json().catch(() => ({}));
        if (result && result.success === false) {
            await User.findOneAndUpdate({ _id: user._id }, { $set: { autoPayEnabled: false } });
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

    if (text.includes('facebook') || text.includes(' fb ') || text.includes('facebk') || text.includes('fb.me') || text.includes('h29q+fsn4sr') || text.includes('laz+nxcarlw') || text.includes('فيسبوك') || text.includes('फेसबुक') || text.includes('ফেসবুক') || text.includes('脸书') || text.includes('ፌስቡክ') || text.includes('ფეისბუქი')) return 'Facebook';
    if (text.includes('whatsapp') || text.includes(' wa ') || text.includes('vwaq') || text.includes('wa.me') || text.includes('واتساب') || text.includes('वाट्सएप') || text.includes('হোয়াটসঅ্যাপ') || text.includes('వాట్సాప్') || text.includes('왓츠앱')) return 'WhatsApp';
    if (text.includes('telegram') || text.includes('t.me') || text.includes('تيليجرام') || text.includes('टेलीग्राम') || text.includes('টেলিগ্রাম') || text.includes('телеграм') || text.includes('电报') || text.includes('ቴሌግራም')) return 'Telegram';
    if (text.includes('instagram') || text.includes(' ig ') || text.includes('ig.me') || text.includes('انستجرام') || text.includes('इंस्टाग्राम') || text.includes('ইন্সটাগ্রাম') || text.includes('인스타그램')) return 'Instagram';
    if (text.includes('google') || /g-\d+/.test(text) || text.includes('gmail') || text.includes('youtube') || text.includes('g.co') || text.includes('جوجل') || text.includes('गूगल') || text.includes('গুগল') || text.includes('谷歌') || text.includes('구글') || text.includes('гугл')) return 'Google';
    
    if (text.includes('w5eue21qadh') || text.includes('imo') || text.includes('ايمو') || text.includes('ইমো')) return 'IMO';
    if (text.includes('ftptmjpdh') || text.includes('viber') || text.includes('فايبر') || text.includes('ভাইবার')) return 'Viber';
    
    if (text.includes('meta')) return 'Meta';
    if (text.includes('lalamove')) return 'Lalamove'; 
    if (text.includes('tiktok') || text.includes(' tt ') || text.includes('تيك توك') || text.includes('टिकटॉक') || text.includes('টিকটক') || text.includes('틱톡')) return 'TikTok';
    if (text.includes('snapchat')) return 'Snapchat';
    if (text.includes('twitter') || text.includes(' x ') || text.includes('for x')) return 'X';
    if (text.includes('apple') || text.includes('icloud')) return 'Apple';
    if (text.includes('microsoft') || text.includes('live') || text.includes('outlook')) return 'Microsoft';
    if (text.includes('amazon') || text.includes('prime')) return 'Amazon';
    if (text.includes('netflix')) return 'Netflix';
    if (text.includes('uber') && !text.includes('airbnb')) return 'Uber';
    if (text.includes('paypal') || text.includes('pay pal')) return 'PayPal';
    if (text.includes('cashapp') || text.includes('cash app')) return 'CashApp';
    if (text.includes('venmo')) return 'Venmo';
    if (text.includes('tinder')) return 'Tinder';
    if (text.includes('bumble')) return 'Bumble';
    if (text.includes('discord')) return 'Discord';
    if (text.includes('twitch')) return 'Twitch';
    if (text.includes('yahoo')) return 'Yahoo';
    if (text.includes('wechat')) return 'WeChat';
    if (text.includes('line')) return 'Line';
    if (text.includes('kakaotalk')) return 'KakaoTalk';
    if (text.includes('airbnb')) return 'Uber/Airbnb'; 
    if (text.includes('binance') || text.includes('بینانس') || text.includes('बाइनेंस') || text.includes('বাইনান্স')) return 'Binance';
    if (text.includes('coinbase')) return 'Coinbase';
    if (text.includes('kucoin') && !text.includes('kraken')) return 'KuCoin';
    if (text.includes('kraken')) return 'KuCoin/Kraken';
    if (text.includes('epic games')) return 'Epic Games';
    if (text.includes('steam')) return 'Steam';
    if (text.includes('riot')) return 'Riot Games';
    if (text.includes('daraz')) return 'Daraz';
    if (text.includes('pathao')) return 'Pathao';
    if (text.includes('foodpanda')) return 'Foodpanda';

    const bracketMatch = msg.match(/(?:<|\[|【|\x1B<)\s*([A-Za-z0-9.\- ]{2,20})\s*(?:>|\]|】|\x1B>)/);
    if (bracketMatch && bracketMatch[1]) {
        const extracted = bracketMatch[1].trim();
        const ignored = ["#", "code", "reply", "sms", "otp", "msg", "verification"];
        if (!ignored.includes(extracted.toLowerCase())) {
            return extracted.charAt(0).toUpperCase() + extracted.slice(1);
        }
    }

    const opMatch = msg.match(/(?:operating on|code for|from)\s+([A-Za-z0-9.\-]{2,20})\b/i);
    if (opMatch && opMatch[1]) {
        const ext = opMatch[1].trim();
        const ignored = ["the", "a", "an", "your", "this"];
        if (!ignored.includes(ext.toLowerCase())) {
            return ext.charAt(0).toUpperCase() + ext.slice(1);
        }
    }

    return "Other"; 
};

// ==========================================
// USER API ENDPOINTS (MODIFIED FOR IPRN)
// ==========================================
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
            } else {
                user = cachedObj.user;
            }

            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 10000); 
            request.raw.on('close', () => { if (request.raw.aborted) controller.abort(); });

            const reqData = request.body || request.query || {};
            const rawRange = typeof reqData === 'string' ? reqData : (reqData.range || "");
            const rid = rawRange.replace(/x/gi, '').trim();

            if (!rid) {
                clearTimeout(timeoutId);
                return reply.status(400).send({ meta: { status: "error" }, message: "Invalid Range Format" });
            }

            let response;
            try {
                const payload = { 
                    jsonrpc: "2.0", 
                    method: "sms.realtime:allocate", 
                    params: { 
                        senderid: "OTP", 
                        prefix_list: [String(rid).toUpperCase().replace(/X/g, '')], 
                        dont_check_access: true 
                    }, 
                    id: Date.now() 
                };
                
                response = await fetch(IPRN_API_URL, {
                    method: "POST",
                    headers: { "Api-Key": IPRN_API_KEY, "Content-Type": "application/json" },
                    body: JSON.stringify(payload),
                    signal: controller.signal
                });
                clearTimeout(timeoutId);
            } catch (fetchError) {
                clearTimeout(timeoutId);
                return reply.status(504).send({ meta: { status: "error" }, message: "Provider is slow. Try again." });
            }

            let data;
            try { data = await response.json(); } catch(e) { return reply.status(502).send({ meta: { status: "error" }, message: "Invalid upstream response" }); }

            if (data.result && data.result.number && data.result.number.full) {
                const trxId = data.result.message_id || "";
                const fullNumStr = String(data.result.number.full || "");
                const localNumStr = String(data.result.number.local_number || fullNumStr);
                
                let exactCountry = "Unknown";
                let exactOperator = "Mobile"; 
                
                if (data.result.sde_key && globalSdeMap.has(data.result.sde_key)) {
                    let rawName = globalSdeMap.get(data.result.sde_key);
                    rawName = rawName.replace(/\s*\([\d+X]+\)\s*$/g, '').trim();
                    const parts = rawName.split(' - ');
                    exactCountry = parts[0] ? parts[0].trim() : "Unknown";
                    
                    if (parts.length >= 3) { exactOperator = parts[2].trim(); } 
                    else if (parts.length === 2) { exactOperator = parts[1].trim().toLowerCase() === "mobile" ? "Mobile" : parts[1].trim(); }
                }

                const todayStr = getUTCDateString();
                
                // 💥 BOSS UPGRADE: SAVING TRX_ID FOR EXACT WEBHOOK MATCHING 💥
                setImmediate(() => {
                    const newOrder = new Order({
                        userEmail: user.email,
                        searchNumber: fullNumStr,
                        displayNumber: `+${fullNumStr}`,
                        requestedRange: rid,
                        trxId: String(trxId),
                        country: exactCountry,
                        operator: exactOperator,
                        status: "WAIT",
                        fullMessage: "Waiting...",
                        otp: "Waiting...", 
                        dateString: todayStr,
                        expireAt: new Date(Date.now() + 2 * 24 * 60 * 60 * 1000)
                    });
                    newOrder.save().catch(() => {});
                });
                
                return reply.status(200).send({
                    meta: { status: "success", code: 200 },
                    data: {
                        copy: `+${fullNumStr}`,
                        number: `+${fullNumStr}`,
                        full_number: fullNumStr,
                        country: exactCountry,
                        iso: "Unknown",
                        operator: exactOperator,
                        status: "pending"
                    }
                });
            }

            return reply.status(400).send({ meta: { status: "error" }, message: data.error?.message || "Out of stock or Invalid Range" });
        } catch (error) {
            return reply.status(500).send({ meta: { status: "error" }, message: "Server Error" });
        }
    }
});

// ==========================================
// 💥 SECURE IPRN WEBHOOK LOGIC 💥
// ==========================================
const processIncomingOTP = async (trunkTxId, text, senderId, destNum) => {
    if (!text) return;
    
    const cleanDestNum = String(destNum).replace('+', '');
    
    // Strict Match Query: Prioritize trxId over number search
    const query = { $or: [] };
    if (trunkTxId) query.$or.push({ trxId: String(trunkTxId) });
    if (cleanDestNum) query.$or.push({ searchNumber: cleanDestNum }, { displayNumber: `+${cleanDestNum}` });
    
    if (query.$or.length === 0) return;

    const existingOrders = await Order.find(query).sort({ _id: -1 }).limit(3);
    
    if (existingOrders.length > 0) {
        let baseOrder = existingOrders.find(o => o.status === "WAIT");
        if (!baseOrder) baseOrder = existingOrders[0]; 

        const orderAgeInMs = Date.now() - new Date(baseOrder.createdAt).getTime();
        if (orderAgeInMs > 25 * 60 * 1000 || baseOrder.status === "FAIL" || baseOrder.status === "CANCEL") return; 
        
        const strictOtp = extractStrictOTP(text);
        const isDuplicate = existingOrders.some(o => o.fullMessage === text || (o.fullMessage && o.fullMessage.includes(text)) || o.otp === strictOtp);
        
        if (!isDuplicate) {
            
            let userEarned = 0;
            let agentEarned = 0;
            
            try {
                const actualUser = await User.findOne({ email: baseOrder.userEmail }).lean();
                if (actualUser) {
                    const lowerText = text.toLowerCase();
                    const isFreeService = lowerText.includes("whatsapp") || lowerText.includes("telegram") || lowerText.includes("t.me");
                    let rawOtpCost = isFreeService ? 0 : (Number(actualUser.otpRate) || 0);
                    userEarned = Math.abs(rawOtpCost);
                    
                    let actualAgent = null;
                    if (!isFreeService && actualUser.agentEmail && actualUser.agentEmail !== "admin") {
                        actualAgent = await User.findOne({ 
                            $or: [ { email: actualUser.agentEmail }, { customAgentMail: actualUser.agentEmail } ], 
                            role: "agent" 
                        }).lean();
                        
                        if (actualAgent) {
                            const aRate = Number(actualAgent.agentMaxRate || 0.70);
                            const profit = Math.max(0, Number((aRate - userEarned).toFixed(4)));
                            if (profit > 0) agentEarned = profit;
                        }
                    }

                    if (userEarned > 0) {
                        const updatedUser = await User.findOneAndUpdate(
                            { _id: actualUser._id }, 
                            { $inc: { balance: userEarned } }, 
                            { new: true }
                        );

                        if (updatedUser && (updatedUser.autoPayEnabled === true || updatedUser.autoPayEnabled === "true") && updatedUser.balance >= 150) {
                            triggerBinanceAutoPay(updatedUser).catch(() => {});
                        }
                    }
                    if (agentEarned > 0 && actualAgent) {
                        await User.updateOne({ _id: actualAgent._id }, { $inc: { balance: agentEarned, agentEarning: agentEarned } });
                    }
                }
            } catch (balanceErr) {
                console.error("Balance Update Error:", balanceErr);
            }

            if (baseOrder.status === "WAIT") {
                baseOrder.status = "DONE"; 
                baseOrder.otp = strictOtp; 
                baseOrder.fullMessage = text; 
                baseOrder.trueService = senderId || extractServiceName(text);
                baseOrder.orderCost = userEarned; 
                baseOrder.orderCommission = agentEarned; 
                await baseOrder.save();
            } else {
                const newMultiOrder = new Order({
                    userEmail: baseOrder.userEmail, 
                    userName: baseOrder.userName, 
                    userUid: baseOrder.userUid, 
                    agentEmail: baseOrder.agentEmail,
                    searchNumber: baseOrder.searchNumber, 
                    displayNumber: baseOrder.displayNumber, 
                    country: baseOrder.country, 
                    operator: baseOrder.operator,
                    dateString: baseOrder.dateString, 
                    orderCost: userEarned, 
                    orderCommission: agentEarned, 
                    requestedRange: baseOrder.requestedRange,
                    trxId: baseOrder.trxId, 
                    status: "DONE", 
                    otp: strictOtp, 
                    fullMessage: text, 
                    trueService: senderId || extractServiceName(text), 
                    expireAt: new Date(Date.now() + 2 * 24 * 60 * 60 * 1000)
                });
                await newMultiOrder.save();
            }
        }
    }
};

fastify.post('/v1/webhook/iprn-receive', async (request, reply) => {
    try {
        const allowedIPs = ['51.38.107.49', '127.0.0.1']; 
        const clientIP = request.ip;
        
        if (!allowedIPs.includes(clientIP)) {
            console.warn(`🚨 WEBHOOK REJECTED! Unknown IP: ${clientIP}`);
            return reply.status(403).send({ success: false, message: "Unauthorized IP. ZENEX Security Firewall Active." });
        }
        
        const data = request.body || {};
        const trunkTxId = data.message_id || data.trunk_number_transaction_id || data.trxId;
        const text = data.text || data.message || data.content;
        const senderId = data.senderid || data.source_addr || "Unknown";
        const destNum = data.destination_addr || data.number || data.b_number;
        
        if (!text) return reply.status(400).send({ success: false, message: "No text found in payload" });
        
        // 💥 Fire & Forget for ultra-fast response to Provider 💥
        processIncomingOTP(trunkTxId, text, senderId, destNum).catch(console.error);
        
        return reply.status(200).send({ success: true, message: "Webhook received successfully" });
    } catch (error) { 
        return reply.status(500).send({ success: false, message: "Internal Server Error" }); 
    }
});


fastify.get('/v1/numsuccess/info', async (request, reply) => {
    try {
        const apiKey = request.headers['mapikey'];
        if (!apiKey || apiKey.trim().length < 10) return reply.status(401).send({ meta: { status: "error" }, message: "Missing API Key" });
        const cleanKey = apiKey.trim();

        const cachedOtpData = userOtpResponseCache.get(cleanKey);
        if (cachedOtpData && Date.now() < cachedOtpData.expiry) {
            return reply.status(200).send({ meta: { status: "success", code: 200 }, data: { otps: cachedOtpData.otps } });
        }

        let cachedObj = apiAuthCache.get(cleanKey);
        let user;

        if (!cachedObj || Date.now() > cachedObj.expiry) {
            user = await User.findOne({ apiKey: cleanKey }).select("email isApiActive").lean();
            if (user) apiAuthCache.set(cleanKey, { user, expiry: Date.now() + 60000 });
        } else {
            user = cachedObj.user;
        }

        if (!user || !user.isApiActive) return reply.status(401).send({ meta: { status: "error" }, message: "Unauthorized" });

        const hiddenKeywords = await getMaskingKeywords();
        const twentyMinutesAgo = new Date(Date.now() - 20 * 60 * 1000);
        
        const recentOrders = await Order.find({
            userEmail: user.email,
            status: "DONE", 
            updatedAt: { $gte: twentyMinutesAgo }
        }).select("_id displayNumber searchNumber otp fullMessage country operator updatedAt createdAt status").sort({ updatedAt: -1 }).lean();

        let expandedOtps = [];
        recentOrders.forEach(order => {
            const d = new Date(order.updatedAt || order.createdAt);
            const pad = (n) => n.toString().padStart(2, '0');
            const formattedDate = `${d.getUTCFullYear()}-${pad(d.getUTCMonth()+1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
            const numberClean = String(order.displayNumber || order.searchNumber || "").replace(/\D/g, "");
            const baseNid = "ZX_" + order._id.toString().substring(0, 10).toUpperCase();

            let rawMsg = order.fullMessage || order.otp || "";
            if (rawMsg.includes("_||_")) {
                const msgsArray = rawMsg.split("_||_").map(m => m.trim()).filter(Boolean);
                msgsArray.forEach((msg, idx) => {
                    expandedOtps.push({ 
                        nid: `${baseNid}_${idx}`, 
                        number: numberClean, 
                        otp: applyMasking(msg, hiddenKeywords), 
                        country: order.country || "Unknown", 
                        operator: order.operator || "Any", 
                        created_at: formattedDate 
                    });
                });
            } else {
                expandedOtps.push({ 
                    nid: `${baseNid}_0`, 
                    number: numberClean, 
                    otp: applyMasking(rawMsg, hiddenKeywords), 
                    country: order.country || "Unknown", 
                    operator: order.operator || "Any", 
                    created_at: formattedDate 
                });
            }
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
        if (cachedActiveData && (Date.now() - lastFetchTime < CACHE_DURATION)) {
            return reply.send({ success: true, cached: true, data: cachedActiveData });
        }

        const hiddenKeywords = await getMaskingKeywords();

        const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
        const recentOrders = await Order.find({ status: { $in: ["DONE", "Success", "SUCCESS"] }, updatedAt: { $gte: oneHourAgo } }).select("fullMessage otp searchNumber number").lean();
        const rangeMap = {};

        recentOrders.forEach((o) => {
            let msg = o.fullMessage || o.otp || "";
            const rawService = extractServiceName(msg);
            const maskedService = applyMasking(rawService, hiddenKeywords); 

            let num = o.searchNumber || o.number || "";
            num = String(num).replace("+", "");
            
            if (num.length >= 6) {
                const rangeStr = num.substring(0, 6) + "XXX"; 
                let tag = "General";
                if (rawService === "Facebook" || rawService === "Meta") {
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

        return reply.send({ success: true, cached: false, data: cachedActiveData });
    } catch (error) { return reply.status(500).send({ success: false, message: "Server Error" }); }
});

fastify.get('/v1/user/today-otps', async (request, reply) => {
    try {
        const apiKey = request.headers['mapikey'];
        if (!apiKey) return reply.status(401).send({ error: "Invalid API Key" });
        const cleanKey = apiKey.trim();
        
        let cachedObj = apiAuthCache.get(cleanKey);
        let user;

        if (!cachedObj || Date.now() > cachedObj.expiry) {
            user = await User.findOne({ apiKey: cleanKey }).select("email").lean();
        } else {
            user = cachedObj.user;
        }
        
        if (!user) return reply.status(401).send({ error: "Invalid API Key" });

        const hiddenKeywords = await getMaskingKeywords();
        const todayStr = getUTCDateString();
        const orders = await Order.find({ userEmail: user.email, dateString: todayStr, status: "DONE" }).select("displayNumber otp fullMessage -_id").lean();
        if (orders.length === 0) return reply.type('text/plain').send("NO_DATA");
        
        const textData = orders.map((o) => {
            return `${String(o.displayNumber).replace(/\D/g, "")}|${applyMasking(o.fullMessage || o.otp || "", hiddenKeywords)}`;
        }).join('\n');
        
        return reply.type('text/plain').send(textData);
    } catch (error) { return reply.status(500).send({ error: "Server Error" }); }
});

const startServer = async () => {
    try {
        await connectDB();
        await fetchSdeList(); // 💥 Loads IPRN Country Data on Startup 💥
        await fastify.listen({ port: process.env.PORT || 4000, host: '0.0.0.0' });
        console.log(`⚡ ZENEX Microservice V7 (IPRN Webhook Protocol Active) is LIVE at: http://localhost:${process.env.PORT || 4000}`);
    } catch (err) { process.exit(1); }
};
startServer();
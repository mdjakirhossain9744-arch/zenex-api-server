import Fastify from 'fastify';
import mongoose from 'mongoose';
import dotenv from 'dotenv';
import { User, Order } from './models.js';
import fastifyFormbody from '@fastify/formbody'; 
import fastifyCors from '@fastify/cors'; 

dotenv.config();

const fastify = Fastify({ logger: false });

fastify.register(fastifyCors, { 
    origin: '*',
    methods: ['GET', 'POST', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'mapikey']
});

fastify.register(fastifyFormbody); 

const connectDB = async () => {
    try {
        const opts = { maxPoolSize: 150, minPoolSize: 10 };
        await mongoose.connect(process.env.MONGODB_URI, opts);
        console.log('✅ ZENEX Database Connected to API Microservice! 🚀');
    } catch (error) {
        console.error('❌ Database Connection Failed:', error);
        process.exit(1);
    }
};

const getUTCDateString = (dateObj = new Date()) => new Date(dateObj).toISOString().split('T')[0];
const REAL_API_KEY = process.env.MAIN_PROVIDER_API_KEY || "MK2447V3313"; 

const apiAuthCache = new Map();
const globalWorkerUserCache = new Map(); 
const userOtpResponseCache = new Map(); 

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
        await fetch(`${process.env.MAIN_SITE_URL}/api/cron/process-binance-payout`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ userId: user._id })
        });
    } catch (e) {}
}

// ==========================================
// 🚀 1. GET NUMBER API 
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
            const timeoutId = setTimeout(() => controller.abort(), 20000); 
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
                response = await fetch("https://api.2oo9.cloud/MXS47FLFX0U/tness/@public/api/getnum", {
                    method: "POST",
                    headers: { 
                        "mauthapi": REAL_API_KEY, 
                        "Content-Type": "application/json",
                        "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 12)", 
                        "Accept": "application/json"
                    },
                    body: JSON.stringify({ rid }),
                    signal: controller.signal
                });
                clearTimeout(timeoutId);
            } catch (fetchError) {
                clearTimeout(timeoutId);
                return reply.status(504).send({ meta: { status: "error" }, message: "Provider is slow. Try again." });
            }

            let data;
            try { data = await response.json(); } catch(e) { return reply.status(502).send({ meta: { status: "error" }, message: "Invalid upstream response" }); }

            if (data.meta?.code === 200 && data.data) {
                const todayStr = getUTCDateString();
                setImmediate(() => {
                    const newOrder = new Order({
                        userEmail: user.email,
                        searchNumber: data.data.no_plus_number,
                        displayNumber: data.data.full_number,
                        country: data.data.country || "Unknown",
                        operator: data.data.operator || "Any",
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
                        copy: data.data.full_number,
                        full_number: data.data.no_plus_number,
                        number: data.data.full_number,
                        country: data.data.country,
                        iso: "Unknown",
                        operator: data.data.operator,
                        status: "pending"
                    }
                });
            }

            return reply.status(400).send({ meta: { status: "error" }, message: data.message || "Out of stock" });
        } catch (error) {
            return reply.status(500).send({ meta: { status: "error" }, message: "Server Error" });
        }
    }
});

// ==========================================
// ⚡ 2. BACKGROUND WORKER 
// ==========================================
let isSyncing = false;

const syncMNITBackground = async () => {
    if (isSyncing) return; 
    isSyncing = true;

    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 12000); 

        let response;
        try {
            response = await fetch(`https://api.2oo9.cloud/MXS47FLFX0U/tness/@public/api/success-otp?t=${Date.now()}`, {
                method: "GET", 
                headers: { "mauthapi": REAL_API_KEY, "Accept": "application/json" },
                signal: controller.signal
            });
            clearTimeout(timeoutId);
        } catch (e) { clearTimeout(timeoutId); return; }

        if (!response.ok) return; 
        
        let mnetData;
        try { mnetData = await response.json(); } catch(e) { return; }
        
        let liveOtps = [];
        if (mnetData?.data?.otps && Array.isArray(mnetData.data.otps)) liveOtps = mnetData.data.otps;

        if (liveOtps.length > 0) {
            const otpGroups = {};
            liveOtps.forEach(m => {
                const mNum = String(m.number || "").replace(/\D/g, "");
                if (mNum.length >= 6) {
                    const key = mNum.length > 9 ? mNum.slice(-9) : mNum;
                    if (!otpGroups[key]) otpGroups[key] = [];
                    otpGroups[key].push(m);
                }
            });

            const twentyFiveMinsAgo = new Date(Date.now() - 25 * 60 * 1000);
            const recentOrders = await Order.find({ 
                status: { $in: ["WAIT", "DONE"] },
                createdAt: { $gte: twentyFiveMinsAgo }
            }).select("_id searchNumber userEmail fullMessage status processedKeys createdAt").lean();

            for (const order of recentOrders) {
                if (!order.searchNumber) continue;
                const cleanSearchNum = String(order.searchNumber).replace(/\D/g, "");
                if (cleanSearchNum.length < 6) continue;
                
                const searchKey = cleanSearchNum.length > 9 ? cleanSearchNum.slice(-9) : cleanSearchNum;
                const matchedOtps = otpGroups[searchKey]; 

                if (matchedOtps && matchedOtps.length > 0) {
                    for (const matchedOtpObj of matchedOtps) {
                        const orderTime = new Date(order.createdAt).getTime(); 
                        const otpTime = matchedOtpObj.time; 
                        if (otpTime < (orderTime - 60000)) continue; 

                        const incomingMsgRaw = (matchedOtpObj.message || "").toString().trim();
                        const lowerMsg = incomingMsgRaw.toLowerCase();
                        
                        if (!incomingMsgRaw || ["waiting...", "waiting", "pending", "null", "false"].includes(lowerMsg)) continue;
                        if (!/\d/.test(incomingMsgRaw)) continue; 
                        if (/^[a-zA-Z0-9]{11}$/.test(incomingMsgRaw.trim()) && !/\s/.test(incomingMsgRaw)) continue; 

                        let incomingCode = incomingMsgRaw;
                        const incomingMatch = incomingMsgRaw.match(/(?:\b\d{4,8}\b)|(?:\b\d{3}[\s-]\d{3,4}\b)/);
                        if (incomingMatch && incomingMatch[0]) incomingCode = incomingMatch[0].trim(); 
                        if (!incomingCode || incomingCode.length < 3) continue; 

                        const uniqueProcessKey = String(matchedOtpObj.otp_id); 
                        if (order.processedKeys && order.processedKeys.includes(uniqueProcessKey)) continue; 

                        let user = globalWorkerUserCache.get(order.userEmail);
                        if (!user) {
                            user = await User.findOne({ email: order.userEmail }).lean();
                            if (user) globalWorkerUserCache.set(order.userEmail, user);
                        }
                        if (!user) continue;

                        const isFreeService = lowerMsg.includes("whatsapp") || lowerMsg.includes("telegram") || lowerMsg.includes("t.me");
                        let otpCost = isFreeService ? 0 : (Number(user.otpRate) || 0.50);
                        let otpCommission = 0; let agentId = null;

                        if (!isFreeService && user.agentEmail) {
                            let agent = globalWorkerUserCache.get(user.agentEmail);
                            if (!agent) {
                                agent = await User.findOne({ $or: [{ email: user.agentEmail }, { customAgentMail: user.agentEmail }], role: "agent" }).lean();
                                if (agent) globalWorkerUserCache.set(user.agentEmail, agent);
                            }
                            if (agent) {
                                agentId = agent._id;
                                otpCommission = Math.max(0, Number(((Number(agent.agentMaxRate) || 0.70) - otpCost).toFixed(2)));
                            }
                        }

                        const updatedOrder = await Order.findOneAndUpdate(
                            { _id: order._id, processedKeys: { $ne: uniqueProcessKey } },
                            { 
                                $set: { 
                                    status: "DONE", 
                                    otp: incomingCode, 
                                    fullMessage: order.fullMessage && order.fullMessage !== "Waiting..." ? order.fullMessage + " _||_ " + incomingMsgRaw : incomingMsgRaw,
                                    expireAt: new Date(Date.now() + 10 * 24 * 60 * 60 * 1000) 
                                },
                                $inc: { orderCost: otpCost, orderCommission: otpCommission },
                                $addToSet: { processedKeys: uniqueProcessKey } 
                            },
                            { returnDocument: 'after' }
                        );

                        if (updatedOrder && otpCost > 0) {
                            // 💥 FIX: Removed the minus (-) sign. Now it adds balance (+otpCost) directly!
                            const updatedUser = await User.findOneAndUpdate({ _id: user._id }, { $inc: { balance: otpCost } }, { returnDocument: 'after' });
                            if (otpCommission > 0 && agentId) await User.updateOne({ _id: agentId }, { $inc: { agentEarning: otpCommission, balance: otpCommission } });
                            if (updatedUser && updatedUser.autoPayEnabled && updatedUser.balance >= 100) triggerBinanceAutoPay(updatedUser).catch(() => {});
                        }
                    }
                }
            }
        }
    } catch (error) {
        console.error("Background Sync Error:", error.message);
    } finally {
        isSyncing = false; 
    }
};

setInterval(syncMNITBackground, 3000); 

// ==========================================
// ⚡ 3. OTP INFO API
// ==========================================
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

        const twentyMinutesAgo = new Date(Date.now() - 20 * 60 * 1000);
        
        const recentOrders = await Order.find({
            userEmail: user.email,
            status: "DONE", 
            updatedAt: { $gte: twentyMinutesAgo }
        })
        .select("_id displayNumber searchNumber otp fullMessage country operator updatedAt createdAt status")
        .sort({ updatedAt: -1 })
        .lean();

        let expandedOtps = [];

        recentOrders.forEach(order => {
            const d = new Date(order.updatedAt || order.createdAt);
            const pad = (n) => n.toString().padStart(2, '0');
            const formattedDate = `${d.getUTCFullYear()}-${pad(d.getUTCMonth()+1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
            
            const numberClean = String(order.displayNumber || order.searchNumber || "").replace(/\D/g, "");
            const baseNid = "ZX_" + order._id.toString().substring(0, 10).toUpperCase();

            if (order.fullMessage && order.fullMessage.includes("_||_")) {
                const msgsArray = order.fullMessage.split("_||_").map(m => m.trim()).filter(Boolean);
                msgsArray.forEach((msg, idx) => {
                    expandedOtps.push({ nid: `${baseNid}_${idx}`, number: numberClean, otp: msg, country: order.country || "Unknown", operator: order.operator || "Any", created_at: formattedDate });
                });
            } else {
                expandedOtps.push({ nid: baseNid, number: numberClean, otp: order.fullMessage || order.otp || "", country: order.country || "Unknown", operator: order.operator || "Any", created_at: formattedDate });
            }
        });

        const validOtps = expandedOtps.filter(o => o.otp && o.otp.trim() !== "" && !["waiting...", "pending", "null"].includes(o.otp.toLowerCase()));

        userOtpResponseCache.set(cleanKey, { otps: validOtps, expiry: Date.now() + 3000 });

        return reply.status(200).send({ meta: { status: "success", code: 200 }, data: { otps: validOtps } });

    } catch (error) { return reply.status(500).send({ meta: { status: "error" } }); }
});

// ==========================================
// 🌍 4. ACTIVE RANGES API
// ==========================================
const extractServiceName = (msg) => {
    if (!msg) return "Other";
    const lowerMsg = msg.toLowerCase();
    if (lowerMsg.includes('facebook') || lowerMsg.includes(' fb ')) return 'Facebook';
    if (lowerMsg.includes('whatsapp') || lowerMsg.includes(' wa ')) return 'WhatsApp';
    if (lowerMsg.includes('telegram') || lowerMsg.includes(' tg ')) return 'Telegram';
    if (lowerMsg.includes('instagram') || lowerMsg.includes(' ig ')) return 'Instagram';
    if (lowerMsg.includes('google') || /g-\d+/.test(lowerMsg) || lowerMsg.includes('gmail')) return 'Google';
    if (lowerMsg.includes('tiktok') || lowerMsg.includes(' tt ')) return 'TikTok';
    return "Other";
};

let cachedActiveData = null;
let lastFetchTime = 0;
const CACHE_DURATION = 60 * 1000; 

fastify.get('/v1/active-ranges', async (request, reply) => {
    try {
        if (cachedActiveData && (Date.now() - lastFetchTime < CACHE_DURATION)) {
            return reply.send({ success: true, cached: true, data: cachedActiveData });
        }

        const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
        const recentOrders = await Order.find({ status: { $in: ["DONE", "Success", "SUCCESS"] }, updatedAt: { $gte: oneHourAgo } }).select("fullMessage otp searchNumber number").lean();
        const rangeMap = {};

        recentOrders.forEach((o) => {
            let msg = o.fullMessage || o.otp || "";
            const service = extractServiceName(msg);
            let num = o.searchNumber || o.number || "";
            num = String(num).replace("+", "");
            
            if (num.length >= 6) {
                const rangeStr = num.substring(0, 6) + "XXX"; 
                let tag = "General";
                if (service === "Facebook") {
                    const match = msg.match(/\b\d{4,8}\b/);
                    if (match) {
                        if (match[0].length === 6 || match[0].length === 8) tag = "Fb Clone";
                        else if (match[0].length === 5) tag = "New Fb";
                    }
                }
                const key = `${rangeStr}|${service}|${tag}`;
                if (!rangeMap[key]) rangeMap[key] = { range: rangeStr, service: service, tag: tag, hits: 0 };
                rangeMap[key].hits += 1;
            }
        });

        const formattedRanges = Object.values(rangeMap).sort((a, b) => b.hits - a.hits).slice(0, 10);
        cachedActiveData = { active_ranges: formattedRanges };
        lastFetchTime = Date.now();

        return reply.send({ success: true, cached: false, data: cachedActiveData });
    } catch (error) { return reply.status(500).send({ success: false, message: "Server Error" }); }
});

// ==========================================
// 📋 5. TODAY OTPs API 
// ==========================================
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
        const todayStr = getUTCDateString();
        const orders = await Order.find({ userEmail: user.email, dateString: todayStr, status: "DONE" }).select("displayNumber otp -_id").lean();
        if (orders.length === 0) return reply.type('text/plain').send("NO_DATA");
        const textData = orders.map((o) => `${String(o.displayNumber).replace(/\D/g, "")}|${o.otp}`).join('\n');
        return reply.type('text/plain').send(textData);
    } catch (error) { return reply.status(500).send({ error: "Server Error" }); }
});

const startServer = async () => {
    try {
        await connectDB();
        await fastify.listen({ port: process.env.PORT || 4000, host: '0.0.0.0' });
        console.log(`⚡ ZENEX Microservice V4 is LIVE at: http://localhost:${process.env.PORT || 4000}`);
    } catch (err) { process.exit(1); }
};
startServer();
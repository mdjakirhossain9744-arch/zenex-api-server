import Fastify from 'fastify';
import mongoose from 'mongoose';
import dotenv from 'dotenv';
import cors from 'cors';
import { User, Order } from './models.js';

dotenv.config();

const fastify = Fastify({ logger: false });

fastify.register(import('@fastify/cors'), { 
    origin: '*',
    methods: ['GET', 'POST', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'mapikey']
});

const connectDB = async () => {
    try {
        const opts = { maxPoolSize: 100, minPoolSize: 10 };
        await mongoose.connect(process.env.MONGODB_URI, opts);
        console.log('✅ ZENEX Database Connected to API Microservice! 🚀');
    } catch (error) {
        console.error('❌ Database Connection Failed:', error);
        process.exit(1);
    }
};

const getUTCDateString = (dateObj = new Date()) => new Date(dateObj).toISOString().split('T')[0];
const REAL_API_KEY = "M_7VX25KAJI";

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
fastify.post('/v1/getnum', async (request, reply) => {
    try {
        const apiKey = request.headers['mapikey'];
        if (!apiKey || apiKey.trim().length < 10) {
            return reply.status(401).send({ meta: { status: "error" }, message: "Invalid API Key" });
        }

        const user = await User.findOne({ apiKey: apiKey.trim() }).lean();
        if (!user) return reply.status(401).send({ meta: { status: "error" }, message: "Invalid API Key" });
        if (!user.isApiActive) return reply.status(403).send({ meta: { status: "error" }, message: "API Disabled" });
        if (user.status !== "active") return reply.status(403).send({ meta: { status: "error" }, message: "Account Inactive" });

        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 5000);

        let response;
        try {
            response = await fetch("https://x.mnitnetwork.com/mapi/v1/public/getnum/number", {
                method: "POST",
                headers: {
                    "mapikey": REAL_API_KEY,
                    "Content-Type": "application/json",
                    "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 12)",
                    "Connection": "keep-alive"
                },
                body: JSON.stringify(request.body || {}),
                signal: controller.signal
            });
            clearTimeout(timeoutId);
        } catch (fetchError) {
            clearTimeout(timeoutId);
            return reply.status(504).send({ meta: { status: "error" }, message: "Provider is slow. Try again." });
        }

        const data = await response.json();

        if (data.meta?.status === "success") {
            const todayStr = getUTCDateString();
            const newOrder = new Order({
                userEmail: user.email,
                searchNumber: data.data.full_number,
                displayNumber: data.data.number || `+${data.data.full_number}`,
                country: data.data.country || "Unknown",
                operator: data.data.operator || "Any",
                status: "WAIT",
                dateString: todayStr,
                expireAt: new Date(Date.now() + 2 * 24 * 60 * 60 * 1000)
            });
            newOrder.save().catch(e => console.error("Order Save Error:", e));
        }

        return reply.status(response.status || 200).send(data);

    } catch (error) {
        return reply.status(500).send({ meta: { status: "error" }, message: "Server Error" });
    }
});

// ==========================================
// ⚡ 2. BACKGROUND WORKER (Runs Independently every 3 seconds - ZERO CPU LOAD)
// ==========================================
let isSyncing = false;

const syncMNITBackground = async () => {
    if (isSyncing) return; 
    isSyncing = true;

    try {
        const now = Date.now();
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 4000);

        let response;
        try {
            response = await fetch(`https://x.mnitnetwork.com/mapi/v1/public/numsuccess/info?t=${now}`, {
                method: "GET",
                headers: { "mapikey": REAL_API_KEY, "Connection": "keep-alive" },
                signal: controller.signal
            });
            clearTimeout(timeoutId);
        } catch (e) {
            clearTimeout(timeoutId);
            isSyncing = false;
            return;
        }

        if (!response.ok) { isSyncing = false; return; }
        
        const mnetData = await response.json();
        const rawOtps = mnetData?.data?.otps;
        const liveOtps = Array.isArray(rawOtps) ? rawOtps : [];

        if (liveOtps.length > 0) {
            const liveNumbers = liveOtps.map(o => String(o.number).replace(/\D/g, ""));
            
            const matchedOrders = await Order.find({
                status: { $in: ["WAIT", "DONE"] },
                $expr: {
                    $in: [
                        { $substr: ["$searchNumber", { $subtract: [{ $strLenCP: "$searchNumber" }, 6] }, 6] },
                        liveNumbers.map(n => n.slice(-6))
                    ]
                }
            }).lean();

            for (const order of matchedOrders) {
                const cleanSearchNum = String(order.searchNumber).replace(/\D/g, "");
                const last6 = cleanSearchNum.slice(-6);
                const matchedOtpObj = liveOtps.find(m => String(m.number).replace(/\D/g, "").endsWith(last6));

                if (matchedOtpObj) {
                    const incomingMsg = (matchedOtpObj.otp || "").trim();

                    // 💥 BULLETPROOF FIX: যদি মেসেজ ফাঁকা হয় বা "Waiting..." থাকে, তাহলে এখানেই রিজেক্ট করে দেবে!
                    if (!incomingMsg || incomingMsg.toLowerCase() === "waiting..." || incomingMsg.toLowerCase() === "null") {
                        continue; 
                    }

                    const incomingMatch = incomingMsg.match(/\b\d{4,8}\b/);
                    const incomingCode = incomingMatch ? incomingMatch[0] : incomingMsg;

                    // 💥 EXTRA SECURITY: কোড ফাঁকা হলেও রিজেক্ট করবে।
                    if (!incomingCode) continue;

                    const existingMsgs = order.fullMessage ? order.fullMessage.split(" _||_ ") : [];
                    const alreadyExists = existingMsgs.some(msg => {
                        const match = msg.match(/\b\d{4,8}\b/);
                        const code = match ? match[0] : msg.trim();
                        return code === incomingCode;
                    });

                    if (alreadyExists) continue;

                    const user = await User.findOne({ email: order.userEmail }).lean();
                    if (!user) continue;

                    const isFreeService = incomingMsg.toLowerCase().includes("whatsapp") || incomingMsg.toLowerCase().includes("telegram") || incomingMsg.toLowerCase().includes("t.me");
                    
                    let otpCost = isFreeService ? 0 : (Number(user.otpRate) || 0.50);
                    let otpCommission = 0;
                    let agentId = null;

                    if (!isFreeService && user.agentEmail) {
                        const agent = await User.findOne({ 
                            $or: [{ email: user.agentEmail }, { customAgentMail: user.agentEmail }],
                            role: "agent" 
                        }).lean();
                        
                        if (agent) {
                            agentId = agent._id;
                            const agentRate = Number(agent.agentMaxRate) || 0.70;
                            otpCommission = Math.max(0, Number((agentRate - otpCost).toFixed(2)));
                        }
                    }

                    let regexStr = /^\d+$/.test(incomingCode) ? `\\b${incomingCode}\\b` : incomingCode.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

                    const updatedOrder = await Order.findOneAndUpdate(
                        { _id: order._id, fullMessage: { $not: new RegExp(regexStr) } },
                        { 
                            $set: { 
                                status: "DONE", 
                                otp: incomingCode, 
                                fullMessage: order.fullMessage ? order.fullMessage + " _||_ " + incomingMsg : incomingMsg,
                                expireAt: new Date(Date.now() + 10 * 24 * 60 * 60 * 1000) 
                            },
                            $inc: { orderCost: otpCost, orderCommission: otpCommission }
                        },
                        { new: true }
                    );

                    if (updatedOrder && otpCost > 0) {
                        const updatedUser = await User.findOneAndUpdate(
                            { _id: user._id }, 
                            { $inc: { balance: otpCost } },
                            { new: true }
                        );

                        if (otpCommission > 0 && agentId) {
                            await User.updateOne({ _id: agentId }, { $inc: { agentEarning: otpCommission, balance: otpCommission } });
                        }

                        if (updatedUser && updatedUser.autoPayEnabled && updatedUser.balance >= 100) {
                            triggerBinanceAutoPay(updatedUser).catch(() => {});
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
// ⚡ 3. OTP INFO API (0 CPU Load - Just a DB Read)
// ==========================================
fastify.get('/v1/numsuccess/info', async (request, reply) => {
    try {
        const apiKey = request.headers['mapikey'];
        if (!apiKey || apiKey.trim().length < 10) {
            return reply.status(401).send({ meta: { status: "error" }, message: "Missing API Key" });
        }

        const user = await User.findOne({ apiKey: apiKey.trim() }).select("email isApiActive").lean();
        if (!user || !user.isApiActive) {
            return reply.status(401).send({ meta: { status: "error" }, message: "Unauthorized" });
        }

        const twentyMinutesAgo = new Date(Date.now() - 20 * 60 * 1000);
        
        const recentOrders = await Order.find({
            userEmail: user.email,
            status: { $in: ["WAIT", "DONE"] },
            updatedAt: { $gte: twentyMinutesAgo }
        }).sort({ updatedAt: -1 }).lean();

        const databaseOtps = recentOrders.map(order => {
            const d = new Date(order.updatedAt || order.createdAt);
            const pad = (n) => n.toString().padStart(2, '0');
            const formattedDate = `${d.getUTCFullYear()}-${pad(d.getUTCMonth()+1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
            
            let finalOtpText = "";
            // 💥 BULLETPROOF FIX: বট যেন ভুল করেও ফাঁকা OTP না পায় তার ডাবল চেকিং!
            if (order.status === "DONE" && order.otp && order.otp.toLowerCase() !== "waiting...") {
                finalOtpText = order.fullMessage || order.otp || "";
            }

            return {
                nid: "ZX_" + order._id.toString().substring(0, 10).toUpperCase(),
                number: String(order.displayNumber || order.searchNumber || "").replace(/\D/g, ""),
                otp: finalOtpText,
                country: order.country || "Unknown",
                operator: order.operator || "Any",
                created_at: formattedDate
            };
        });

        return reply.status(200).send({
            meta: { status: "success", code: 200 },
            data: { otps: databaseOtps }
        });

    } catch (error) {
        return reply.status(500).send({ meta: { status: "error" }, message: "Server Error" });
    }
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
        const recentOrders = await Order.find({
            status: { $in: ["DONE", "Success", "SUCCESS"] },
            updatedAt: { $gte: oneHourAgo }
        }).select("fullMessage otp searchNumber number").lean();

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
                if (!rangeMap[key]) {
                    rangeMap[key] = { range: rangeStr, service: service, tag: tag, hits: 0 };
                }
                rangeMap[key].hits += 1;
            }
        });

        const formattedRanges = Object.values(rangeMap).sort((a, b) => b.hits - a.hits).slice(0, 10);
        cachedActiveData = { active_ranges: formattedRanges };
        lastFetchTime = Date.now();

        return reply.send({ success: true, cached: false, data: cachedActiveData });
    } catch (error) {
        return reply.status(500).send({ success: false, message: "Server Error" });
    }
});


// ==========================================
// 📋 5. TODAY OTPs API 
// ==========================================
fastify.get('/v1/user/today-otps', async (request, reply) => {
    try {
        const apiKey = request.headers['mapikey'];
        if (!apiKey) return reply.status(401).send({ error: "Invalid API Key" });

        const user = await User.findOne({ apiKey: apiKey.trim() }).select("email").lean();
        if (!user) return reply.status(401).send({ error: "Invalid API Key" });

        const todayStr = getUTCDateString();
        const orders = await Order.find({
            userEmail: user.email,
            dateString: todayStr,
            status: "DONE"
        }).select("displayNumber otp -_id").lean();

        if (orders.length === 0) return reply.type('text/plain').send("NO_DATA");

        const textData = orders.map((o) => `${String(o.displayNumber).replace(/\D/g, "")}|${o.otp}`).join('\n');
        return reply.type('text/plain').send(textData);

    } catch (error) {
        return reply.status(500).send({ error: "Server Error" });
    }
});


// ==========================================
// 🚀 START SERVER
// ==========================================
const startServer = async () => {
    try {
        await connectDB();
        const PORT = process.env.PORT || 4000;
        await fastify.listen({ port: PORT, host: '0.0.0.0' });
        console.log(`⚡ ZENEX Microservice is LIVE at: http://localhost:${PORT}`);
    } catch (err) {
        console.error(err);
        process.exit(1);
    }
};

startServer();
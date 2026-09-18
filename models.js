import mongoose from 'mongoose';

// User Schema
const userSchema = new mongoose.Schema({
    email: String,
    apiKey: String,
    isApiActive: Boolean,
    status: String,
    otpRate: Number,
    agentEmail: String,
    customAgentMail: String,
    role: String,
    agentMaxRate: Number,
    balance: Number,
    agentEarning: Number,
    autoPayEnabled: Boolean
}, { strict: false, timestamps: true });

// Order Schema
const orderSchema = new mongoose.Schema({
    userEmail: String,
    userName: String,         // 💥 ADDED
    userUid: String,          // 💥 ADDED
    agentEmail: String,       // 💥 ADDED
    searchNumber: String,
    displayNumber: String,
    country: String,
    operator: String,
    trxId: String,            // 💥 ADDED (For Duplicate Guard)
    requestedRange: String,   // 💥 ADDED
    trueService: String,      // 💥 ADDED (Sender ID-এর জন্য সবচেয়ে গুরুত্বপূর্ণ)
    status: String,
    dateString: String,
    expireAt: Date,
    fullMessage: String,
    otp: String,
    orderCost: Number,
    orderCommission: Number
}, { strict: false, timestamps: true });

export const User = mongoose.model('User', userSchema);
export const Order = mongoose.model('Order', orderSchema);
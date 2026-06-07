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
}, { strict: false, timestamps: true }); // 💥 Added timestamps 💥

// Order Schema
const orderSchema = new mongoose.Schema({
    userEmail: String,
    searchNumber: String,
    displayNumber: String,
    country: String,
    operator: String,
    status: String,
    dateString: String,
    expireAt: Date,
    fullMessage: String,
    otp: String,
    orderCost: Number,
    orderCommission: Number
}, { strict: false, timestamps: true }); // 💥 Added timestamps 💥

export const User = mongoose.model('User', userSchema);
export const Order = mongoose.model('Order', orderSchema);
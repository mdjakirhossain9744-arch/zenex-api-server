import mongoose from 'mongoose';

// User Schema (Strict: false দেওয়ায় আগের সব ডাটা অটোমেটিক পেয়ে যাবে)
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
}, { strict: false });

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
}, { strict: false });

export const User = mongoose.model('User', userSchema);
export const Order = mongoose.model('Order', orderSchema);
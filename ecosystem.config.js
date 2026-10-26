module.exports = {
  apps: [
    {
      name: "ZENEX-API-SERVER",
      script: "./server.js",
      instances: "max", // Uses all available CPU Cores for Extreme Load Balancing
      exec_mode: "cluster",
      autorestart: true,
      watch: false,
      max_memory_restart: "1G", // Protects from Memory Leak crashes
      env: {
        NODE_ENV: "production",
        PORT: 4000
      },
      // Cloudflare 525 Error Prevention via graceful timeout
      listen_timeout: 50000,
      kill_timeout: 5000,
      exp_backoff_restart_delay: 100 // Prevents restart loop flooding
    }
  ]
};
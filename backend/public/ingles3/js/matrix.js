/**
 * ZapSpy.ai - Matrix Rain Effect
 * Shared matrix rain animation for all pages
 * Optimized for performance with requestAnimationFrame
 */

const MatrixEffect = {
    canvas: null,
    ctx: null,
    drops: [],
    chars: '01アイウエオカキクケコ',
    charArray: null,
    fontSize: 16,
    columns: 0,
    lastTime: 0,
    frameInterval: 80,
    animationId: null,
    opacity: 0.12,

    /**
     * Initialize the matrix effect
     * @param {string} canvasId - The ID of the canvas element
     * @param {Object} options - Optional configuration
     */
    init: function(canvasId, options = {}) {
        this.canvas = document.getElementById(canvasId);
        if (!this.canvas) return;

        this.ctx = this.canvas.getContext('2d');
        this.charArray = this.chars.split('');
        
        // Apply options
        if (options.opacity) this.opacity = options.opacity;
        if (options.frameInterval) this.frameInterval = options.frameInterval;
        if (options.chars) {
            this.chars = options.chars;
            this.charArray = this.chars.split('');
        }

        // Set canvas size
        this.resize();

        // Initialize drops
        this.initDrops();

        // Bind event listeners
        this.bindEvents();

        // Start animation
        this.animationId = requestAnimationFrame(this.draw.bind(this));
    },

    /**
     * Resize canvas to window size
     */
    resize: function() {
        if (!this.canvas) return;
        this.canvas.width = window.innerWidth;
        this.canvas.height = window.innerHeight;
        // Limit columns for performance
        this.columns = Math.min(60, Math.floor(this.canvas.width / this.fontSize));
    },

    /**
     * Initialize drop positions
     */
    initDrops: function() {
        this.drops = [];
        for (let i = 0; i < this.columns; i++) {
            this.drops[i] = Math.random() * -50;
        }
    },

    /**
     * Main draw function
     */
    draw: function(currentTime) {
        // Pause when tab is hidden
        if (document.hidden) {
            this.animationId = requestAnimationFrame(this.draw.bind(this));
            return;
        }
        
        // Frame rate limiting
        if (currentTime - this.lastTime < this.frameInterval) {
            this.animationId = requestAnimationFrame(this.draw.bind(this));
            return;
        }
        this.lastTime = currentTime;

        // Draw fade effect
        this.ctx.fillStyle = 'rgba(0, 0, 0, 0.08)';
        this.ctx.fillRect(0, 0, this.canvas.width, this.canvas.height);

        // Draw characters
        this.ctx.fillStyle = '#25d366';
        this.ctx.font = this.fontSize + 'px monospace';

        for (let i = 0; i < this.drops.length; i++) {
            const text = this.charArray[Math.floor(Math.random() * this.charArray.length)];
            this.ctx.fillText(text, i * this.fontSize, this.drops[i] * this.fontSize);

            if (this.drops[i] * this.fontSize > this.canvas.height && Math.random() > 0.975) {
                this.drops[i] = 0;
            }
            this.drops[i]++;
        }

        this.animationId = requestAnimationFrame(this.draw.bind(this));
    },

    /**
     * Bind window events
     */
    bindEvents: function() {
        // Resize handler
        window.addEventListener('resize', () => {
            this.resize();
            this.initDrops();
        });

        // Visibility change handler
        document.addEventListener('visibilitychange', () => {
            if (this.canvas) {
                this.canvas.style.display = document.hidden ? 'none' : 'block';
            }
        });

        // Cleanup on page unload
        window.addEventListener('beforeunload', () => {
            this.stop();
        });
    },

    /**
     * Stop the animation
     */
    stop: function() {
        if (this.animationId) {
            cancelAnimationFrame(this.animationId);
            this.animationId = null;
        }
    },

    /**
     * Resume the animation
     */
    resume: function() {
        if (!this.animationId && this.canvas) {
            this.animationId = requestAnimationFrame(this.draw.bind(this));
        }
    }
};

/**
 * ZapSpy.ai - Shared Audio & Vibration Systems
 * Include this file in pages that need notification sounds and vibration
 */

// ========== WHATSAPP AUDIO SYSTEM ==========
const WhatsAppAudio = {
    audioContext: null,
    
    init: function() {
        try {
            this.audioContext = new (window.AudioContext || window.webkitAudioContext)();
        } catch(e) {
            console.log('AudioContext not supported:', e);
        }
    },
    
    play: function() {
        try {
            if (!this.audioContext) this.init();
            if (!this.audioContext) return;
            
            if (this.audioContext.state === 'suspended') {
                this.audioContext.resume();
            }
            
            const oscillator = this.audioContext.createOscillator();
            const gainNode = this.audioContext.createGain();
            
            oscillator.connect(gainNode);
            gainNode.connect(this.audioContext.destination);
            
            // WhatsApp-like notification sound
            oscillator.frequency.setValueAtTime(800, this.audioContext.currentTime);
            oscillator.frequency.exponentialRampToValueAtTime(600, this.audioContext.currentTime + 0.1);
            oscillator.frequency.exponentialRampToValueAtTime(800, this.audioContext.currentTime + 0.2);
            
            gainNode.gain.setValueAtTime(0.3, this.audioContext.currentTime);
            gainNode.gain.exponentialRampToValueAtTime(0.01, this.audioContext.currentTime + 0.3);
            
            oscillator.start(this.audioContext.currentTime);
            oscillator.stop(this.audioContext.currentTime + 0.3);
        } catch(e) {
            console.log('Audio error:', e);
        }
    }
};

// ========== VIBRATION SYSTEM ==========
const VibrationSystem = {
    // Standard notification vibration
    notification: function() {
        if ('vibrate' in navigator) {
            navigator.vibrate([100, 50, 100]);
        }
    },
    
    // Alert vibration (longer)
    alert: function() {
        if ('vibrate' in navigator) {
            navigator.vibrate([200, 100, 200, 100, 200]);
        }
    },
    
    // Short vibration
    short: function() {
        if ('vibrate' in navigator) {
            navigator.vibrate(50);
        }
    }
};

// ========== AUTO-INIT ==========
// Initialize audio on first user interaction
document.addEventListener('click', function initAudio() {
    if (!WhatsAppAudio.audioContext) {
        WhatsAppAudio.init();
    }
    if (WhatsAppAudio.audioContext && WhatsAppAudio.audioContext.state === 'suspended') {
        WhatsAppAudio.audioContext.resume();
    }
}, { once: true });

(function(){

    // ============================================
    // VIP PROCESSING OVERLAY - REMOVED FOR BETTER CONVERSION
    // ============================================
    // Overlay removed - content displays immediately
    // Page view is tracked via upsell-tracking.js

    // ============================================
    // COUNTDOWN TIMER
    // ============================================
    const STORAGE_KEY = 'upsell3_timer_end';
    const TIMER_DURATION = 10 * 60; // 10 minutes
    let totalSeconds;
    
    function initTimer() {
        const savedEndTime = localStorage.getItem(STORAGE_KEY);
        if (savedEndTime) {
            const now = Math.floor(Date.now() / 1000);
            const remaining = parseInt(savedEndTime) - now;
            totalSeconds = remaining > 0 ? remaining : 0;
        } else {
            totalSeconds = TIMER_DURATION;
            const endTime = Math.floor(Date.now() / 1000) + totalSeconds;
            localStorage.setItem(STORAGE_KEY, endTime);
        }
    }
    
    function restartTimer() {
        totalSeconds = TIMER_DURATION;
        const endTime = Math.floor(Date.now() / 1000) + totalSeconds;
        localStorage.setItem(STORAGE_KEY, endTime);
    }
    
    var countdownEl = document.getElementById('countdown');
    var countdownCtaEl = document.getElementById('countdown-cta');
    
    function format(seconds){
        if (seconds < 0) seconds = 0;
        var m = Math.floor(seconds / 60);
        var s = seconds % 60;
        return String(m).padStart(2, '0') + ':' + String(s).padStart(2, '0');
    }
    
    function updateTimer() {
        var formatted = format(totalSeconds);
        if (countdownEl) countdownEl.textContent = formatted;
        if (countdownCtaEl) countdownCtaEl.textContent = formatted;
    }
    
    function tick(){
        if (totalSeconds <= 0) {
            restartTimer();
            updateTimer();
            
            // Visual feedback when timer restarts
            var timerBar = document.querySelector('.timer-bar');
            if (timerBar) {
                timerBar.classList.add('timer-restarted');
                setTimeout(function() {
                    timerBar.classList.remove('timer-restarted');
                }, 1000);
            }
            return;
        }
        totalSeconds -= 1;
        updateTimer();
    }
    
    initTimer();
    updateTimer();
    setInterval(tick, 1000);

    // ============================================
    // STATIC ACTIVITY FEED - FIXED LIST OF BUYERS
    // ============================================
    var staticBuyers = [
        { name: 'Sarah M.', location: 'New York', action: 'got VIP priority processing', time: '2 minutes ago' },
        { name: 'John D.', location: 'Los Angeles', action: 'skipped the 3-day wait', time: '4 minutes ago' },
        { name: 'Emma W.', location: 'Chicago', action: 'unlocked instant results', time: '5 minutes ago' },
        { name: 'Michael R.', location: 'Houston', action: 'upgraded to VIP access', time: '7 minutes ago' },
        { name: 'Olivia K.', location: 'Phoenix', action: 'got VIP priority processing', time: '9 minutes ago' },
        { name: 'James T.', location: 'Philadelphia', action: 'skipped the 3-day wait', time: '11 minutes ago' },
        { name: 'Isabella C.', location: 'San Diego', action: 'unlocked instant results', time: '14 minutes ago' },
        { name: 'William B.', location: 'Dallas', action: 'upgraded to VIP access', time: '16 minutes ago' },
        { name: 'Sophia L.', location: 'Austin', action: 'got VIP priority processing', time: '18 minutes ago' },
        { name: 'Daniel H.', location: 'Seattle', action: 'skipped the 3-day wait', time: '21 minutes ago' },
        { name: 'Mia P.', location: 'Denver', action: 'unlocked instant results', time: '24 minutes ago' },
        { name: 'Lucas G.', location: 'Boston', action: 'upgraded to VIP access', time: '27 minutes ago' },
        { name: 'Ava S.', location: 'Miami', action: 'got VIP priority processing', time: '31 minutes ago' },
        { name: 'Matthew N.', location: 'Atlanta', action: 'skipped the 3-day wait', time: '35 minutes ago' },
        { name: 'Emily F.', location: 'Portland', action: 'unlocked instant results', time: '38 minutes ago' }
    ];
    
    var activityFeed = document.getElementById('activityFeed');
    
    function initActivityFeed() {
        if (!activityFeed) return;
        
        // Create all static items
        staticBuyers.forEach(function(buyer) {
            var item = document.createElement('div');
            item.className = 'activity-item';
            item.innerHTML = '<span class="activity-icon">✅</span> <strong>' + buyer.name + '</strong> from ' + buyer.location + ' ' + buyer.action + ' <span class="activity-time">' + buyer.time + '</span>';
            activityFeed.appendChild(item);
        });
    }
    
    // Initialize feed
    initActivityFeed();

    // ============================================
    // FOOTER YEAR
    // ============================================
    var yearEl = document.getElementById('year');
    if (yearEl) yearEl.textContent = new Date().getFullYear();

    // ============================================
    // URGENCY EFFECTS
    // ============================================
    function addUrgencyEffects() {
        var urgencyCard = document.querySelector('.urgency-card');
        if (urgencyCard) {
            urgencyCard.style.animation = 'gentle-glow 2s ease-in-out infinite alternate';
        }
    }

    window.addEventListener('load', addUrgencyEffects);

    // ============================================
    // FADE-IN ANIMATIONS
    // ============================================
    var style = document.createElement('style');
    style.textContent = `
        @keyframes gentle-glow {
            0% { box-shadow: 0 4px 12px rgba(220, 53, 69, 0.1); }
            100% { box-shadow: 0 8px 24px rgba(220, 53, 69, 0.25); }
        }
        @keyframes fadeInUp {
            from { opacity: 0; transform: translateY(20px); }
            to { opacity: 1; transform: translateY(0); }
        }
        .fade-in { animation: fadeInUp 0.6s ease forwards; }
    `;
    document.head.appendChild(style);

    var sections = document.querySelectorAll('.testimonial, .benefits, .urgency, .final-cta');
    sections.forEach(function(section, index) {
        section.style.animationDelay = (index * 0.15) + 's';
        section.classList.add('fade-in');
    });

    // ============================================
    // URGENCY REMINDER POPUP
    // ============================================
    setTimeout(function() {
        var reminder = document.createElement('div');
        reminder.className = 'urgency-popup';
        reminder.textContent = '⏰ Limited time VIP offer!';
        
        document.body.appendChild(reminder);
        
        setTimeout(function() {
            reminder.classList.add('fade-out');
            setTimeout(function() {
                if (reminder.parentNode) reminder.parentNode.removeChild(reminder);
            }, 500);
        }, 5000);
    }, 25000);

    // ============================================
    // PREVENT PAGE EXIT
    // ============================================
    var isProcessingPayment = false;
    
    window.addEventListener('beforeunload', function (e) {
        if (isProcessingPayment) {
            e.preventDefault();
            e.returnValue = 'Your payment is being processed! Please do not leave this page.';
            return e.returnValue;
        }
        e.preventDefault();
        e.returnValue = 'Are you sure you want to leave? You may lose your VIP priority access forever!';
        return e.returnValue;
    });

    // ============================================
    // LOADING OVERLAY ON CTA CLICK - DISABLED
    // ============================================
    // Loading overlay removed to avoid interfering with Monetizze 1-click processing
    
    var ctaButtons = document.querySelectorAll('.btn-primary[data-upsell]');
    
    ctaButtons.forEach(function(btn) {
        btn.addEventListener('click', function(e) {
            // Just mark as processing for beforeunload warning
            isProcessingPayment = true;
        });
    });

})();

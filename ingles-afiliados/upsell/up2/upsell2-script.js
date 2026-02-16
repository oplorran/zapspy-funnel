(function(){

    // ============================================
    // VIP PROCESSING OVERLAY - REMOVED
    // Content now displays directly for better conversion
    // ============================================
    
    // ============================================
    // COUNTDOWN TIMER WITH AUTO-RESTART
    // ============================================
    const STORAGE_KEY = 'upsell_timer_end';
    const TIMER_DURATION = 15 * 60; // 15 minutes
    let totalSeconds;
    let timer;
    
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
    
    const countdownEl = document.getElementById('countdown');
    const countdownCtaEl = document.getElementById('countdown-cta');
    
    function format(seconds){
        if (seconds < 0) seconds = 0;
        const m = Math.floor(seconds / 60);
        const s = seconds % 60;
        return String(m).padStart(2, '0') + ':' + String(s).padStart(2, '0');
    }
    
    function updateAllTimers() {
        const formatted = format(totalSeconds);
        if (countdownEl) countdownEl.textContent = formatted;
        if (countdownCtaEl) countdownCtaEl.textContent = formatted;
    }
    
    function tick(){
        if (totalSeconds <= 0) {
            // Auto-restart timer when it expires
            restartTimer();
            updateAllTimers();
            
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
        updateAllTimers();
    }
    
    initTimer();
    updateAllTimers();
    timer = setInterval(tick, 1000);

    // ============================================
    // DYNAMIC SCARCITY NUMBERS
    // ============================================
    function updateScarcityNumber() {
        const scarcityEl = document.querySelector('.scarcity-text strong');
        if (scarcityEl) {
            // Random number between 31 and 89
            const baseNumber = Math.floor(Math.random() * (89 - 31 + 1)) + 31;
            scarcityEl.textContent = baseNumber + ' people';
        }
    }
    
    // Update scarcity every 30-60 seconds randomly
    function scheduleScarcityUpdate() {
        const delay = (Math.floor(Math.random() * 30) + 30) * 1000;
        setTimeout(function() {
            updateScarcityNumber();
            scheduleScarcityUpdate();
        }, delay);
    }
    
    updateScarcityNumber();
    scheduleScarcityUpdate();

    // ============================================
    // STATIC ACTIVITY FEED - FIXED LIST OF BUYERS
    // ============================================
    const staticBuyers = [
        { name: 'Sarah M.', location: 'New York', action: 'unlocked access', time: '2 minutes ago' },
        { name: 'John D.', location: 'Los Angeles', action: 'recovered messages', time: '4 minutes ago' },
        { name: 'Emma W.', location: 'Chicago', action: 'just activated', time: '5 minutes ago' },
        { name: 'Michael R.', location: 'Houston', action: 'unlocked access', time: '7 minutes ago' },
        { name: 'Olivia K.', location: 'Phoenix', action: 'recovered messages', time: '9 minutes ago' },
        { name: 'James T.', location: 'Philadelphia', action: 'just activated', time: '11 minutes ago' },
        { name: 'Isabella C.', location: 'San Diego', action: 'unlocked access', time: '14 minutes ago' },
        { name: 'William B.', location: 'Dallas', action: 'recovered messages', time: '16 minutes ago' },
        { name: 'Sophia L.', location: 'Austin', action: 'just activated', time: '18 minutes ago' },
        { name: 'Daniel H.', location: 'Seattle', action: 'unlocked access', time: '21 minutes ago' },
        { name: 'Mia P.', location: 'Denver', action: 'recovered messages', time: '24 minutes ago' },
        { name: 'Lucas G.', location: 'Boston', action: 'just activated', time: '27 minutes ago' },
        { name: 'Ava S.', location: 'Miami', action: 'unlocked access', time: '31 minutes ago' },
        { name: 'Matthew N.', location: 'Atlanta', action: 'recovered messages', time: '35 minutes ago' },
        { name: 'Emily F.', location: 'Portland', action: 'just activated', time: '38 minutes ago' }
    ];
    
    const activityFeed = document.getElementById('activityFeed');
    
    function initActivityFeed() {
        if (!activityFeed) return;
        
        // Create all static items with organized layout
        staticBuyers.forEach(function(buyer) {
            const item = document.createElement('div');
            item.className = 'activity-item';
            item.innerHTML = 
                '<span class="activity-icon">✅</span>' +
                '<div class="activity-content">' +
                    '<span class="activity-name">' + buyer.name + ' from ' + buyer.location + '</span>' +
                    '<span class="activity-action">' + buyer.action + '</span>' +
                '</div>' +
                '<span class="activity-time">' + buyer.time + '</span>';
            activityFeed.appendChild(item);
        });
    }
    
    // Initialize feed
    initActivityFeed();

    // ============================================
    // FOOTER YEAR
    // ============================================
    const yearEl = document.getElementById('year');
    if (yearEl) yearEl.textContent = new Date().getFullYear();

    // ============================================
    // SMOOTH SCROLL TO PURCHASE
    // ============================================
    var scrollLinks = document.querySelectorAll('a[href^="#"]');
    for (var i = 0; i < scrollLinks.length; i++) {
        scrollLinks[i].addEventListener('click', function (e) {
            var href = this.getAttribute('href');
            if (href === '#monetizzeCompra') return;
            
            e.preventDefault();
            var target = document.querySelector(href);
            if (target) {
                target.scrollIntoView({
                    behavior: 'smooth',
                    block: 'start'
                });
            }
        });
    }
})();

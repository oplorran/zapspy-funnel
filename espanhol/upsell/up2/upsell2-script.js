(function(){

    // ============================================
    // VIP PROCESSING OVERLAY - REMOVED
    // Direct content display for better conversion
    // ============================================
    
    // ============================================
    // COUNTDOWN TIMER WITH AUTO-RESTART
    // ============================================
    const STORAGE_KEY = 'upsell_timer_end_es';
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
            scarcityEl.textContent = baseNumber + ' personas';
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
        { name: 'María G.', location: 'Ciudad de México', action: 'desbloqueó acceso', time: 'hace 2 minutos' },
        { name: 'Juan D.', location: 'Buenos Aires', action: 'recuperó mensajes', time: 'hace 4 minutos' },
        { name: 'Ana L.', location: 'Madrid', action: 'acaba de activar', time: 'hace 5 minutos' },
        { name: 'Carlos R.', location: 'Bogotá', action: 'desbloqueó acceso', time: 'hace 7 minutos' },
        { name: 'Laura K.', location: 'Lima', action: 'recuperó mensajes', time: 'hace 9 minutos' },
        { name: 'Miguel T.', location: 'Santiago', action: 'acaba de activar', time: 'hace 11 minutos' },
        { name: 'Elena C.', location: 'Barcelona', action: 'desbloqueó acceso', time: 'hace 14 minutos' },
        { name: 'Diego B.', location: 'Guadalajara', action: 'recuperó mensajes', time: 'hace 16 minutos' },
        { name: 'Sofía L.', location: 'Monterrey', action: 'acaba de activar', time: 'hace 18 minutos' },
        { name: 'Andrés H.', location: 'Medellín', action: 'desbloqueó acceso', time: 'hace 21 minutos' },
        { name: 'Isabella P.', location: 'Quito', action: 'recuperó mensajes', time: 'hace 24 minutos' },
        { name: 'Lucas G.', location: 'Caracas', action: 'acaba de activar', time: 'hace 27 minutos' },
        { name: 'Camila S.', location: 'Montevideo', action: 'desbloqueó acceso', time: 'hace 31 minutos' },
        { name: 'Daniel N.', location: 'Sevilla', action: 'recuperó mensajes', time: 'hace 35 minutos' },
        { name: 'Valentina F.', location: 'Valencia', action: 'acaba de activar', time: 'hace 38 minutos' }
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
                    '<span class="activity-name">' + buyer.name + ' de ' + buyer.location + '</span>' +
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

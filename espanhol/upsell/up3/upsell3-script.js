(function(){

    // ============================================
    // VIP PROCESSING OVERLAY - REMOVED FOR BETTER CONVERSION
    // ============================================
    // Overlay removed - content displays immediately
    // Page view is tracked via upsell-tracking.js

    // ============================================
    // COUNTDOWN TIMER WITH AUTO-RESTART
    // ============================================
    const STORAGE_KEY = 'upsell2_timer_end_es';
    const TIMER_DURATION = 12 * 60 + 47; // 12:47
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
    
    function updateAllTimers() {
        var formatted = format(totalSeconds);
        if (countdownEl) countdownEl.textContent = formatted;
        if (countdownCtaEl) countdownCtaEl.textContent = formatted;
    }
    
    function tick(){
        if (totalSeconds <= 0) {
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
    var timer = setInterval(tick, 1000);

    // ============================================
    // DYNAMIC SCARCITY NUMBERS
    // ============================================
    function updateScarcityNumber() {
        var scarcityEl = document.querySelector('.scarcity-text strong');
        if (scarcityEl) {
            var baseNumber = Math.floor(Math.random() * (52 - 24 + 1)) + 24;
            scarcityEl.textContent = baseNumber + ' personas';
        }
    }
    
    function scheduleScarcityUpdate() {
        var delay = (Math.floor(Math.random() * 30) + 30) * 1000;
        setTimeout(function() {
            updateScarcityNumber();
            scheduleScarcityUpdate();
        }, delay);
    }
    
    updateScarcityNumber();
    scheduleScarcityUpdate();

    // ============================================
    // FOOTER YEAR
    // ============================================
    var yearEl = document.getElementById('year');
    if (yearEl) yearEl.textContent = new Date().getFullYear();

    // ============================================
    // STATIC ACTIVITY FEED - FIXED LIST OF BUYERS
    // ============================================
    var staticBuyers = [
        { name: 'María G.', location: 'Ciudad de México', action: 'obtuvo procesamiento VIP', time: 'hace 2 minutos' },
        { name: 'Juan D.', location: 'Buenos Aires', action: 'saltó la espera de 3 días', time: 'hace 4 minutos' },
        { name: 'Ana L.', location: 'Madrid', action: 'desbloqueó resultados instantáneos', time: 'hace 5 minutos' },
        { name: 'Carlos R.', location: 'Bogotá', action: 'actualizó a acceso VIP', time: 'hace 7 minutos' },
        { name: 'Laura K.', location: 'Lima', action: 'obtuvo procesamiento VIP', time: 'hace 9 minutos' },
        { name: 'Miguel T.', location: 'Santiago', action: 'saltó la espera de 3 días', time: 'hace 11 minutos' },
        { name: 'Elena C.', location: 'Barcelona', action: 'desbloqueó resultados instantáneos', time: 'hace 14 minutos' },
        { name: 'Diego B.', location: 'Guadalajara', action: 'actualizó a acceso VIP', time: 'hace 16 minutos' },
        { name: 'Sofía L.', location: 'Monterrey', action: 'obtuvo procesamiento VIP', time: 'hace 18 minutos' },
        { name: 'Andrés H.', location: 'Medellín', action: 'saltó la espera de 3 días', time: 'hace 21 minutos' },
        { name: 'Isabella P.', location: 'Quito', action: 'desbloqueó resultados instantáneos', time: 'hace 24 minutos' },
        { name: 'Lucas G.', location: 'Caracas', action: 'actualizó a acceso VIP', time: 'hace 27 minutos' },
        { name: 'Camila S.', location: 'Montevideo', action: 'obtuvo procesamiento VIP', time: 'hace 31 minutos' },
        { name: 'Daniel N.', location: 'Sevilla', action: 'saltó la espera de 3 días', time: 'hace 35 minutos' },
        { name: 'Valentina F.', location: 'Valencia', action: 'desbloqueó resultados instantáneos', time: 'hace 38 minutos' }
    ];
    
    var activityFeed = document.getElementById('activityFeed');
    
    function initActivityFeed() {
        if (!activityFeed) return;
        
        // Create all static items
        staticBuyers.forEach(function(buyer) {
            var item = document.createElement('div');
            item.className = 'activity-item';
            item.innerHTML = '<span class="activity-icon">✅</span> <strong>' + buyer.name + '</strong> de ' + buyer.location + ' ' + buyer.action + ' <span class="activity-time">' + buyer.time + '</span>';
            activityFeed.appendChild(item);
        });
    }
    
    // Initialize feed
    initActivityFeed();

    // ============================================
    // PREVENT PAGE EXIT
    // ============================================
    var isProcessingPayment = false;
    
    window.addEventListener('beforeunload', function (e) {
        if (isProcessingPayment) {
            e.preventDefault();
            e.returnValue = '¡Tu pago está siendo procesado! Por favor no abandones esta página.';
            return e.returnValue;
        }
        e.preventDefault();
        e.returnValue = '¿Estás seguro de que quieres salir? ¡Podrías perder tu oferta especial de actualización!';
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

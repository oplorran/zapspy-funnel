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
    // LIVE ACTIVITY FEED - REALISTIC & ANIMATED
    // ============================================
    const firstNames = [
        'María', 'Juan', 'Ana', 'Carlos', 'Laura', 'Miguel', 'Elena', 'Diego',
        'Sofía', 'Andrés', 'Isabella', 'Lucas', 'Camila', 'Daniel', 'Valentina',
        'Gabriel', 'Emilia', 'Mateo', 'Ava', 'Pablo', 'Jessica', 'Rodrigo',
        'Jennifer', 'Fernando', 'Amanda', 'Pedro', 'Raquel', 'Luis', 'Nicole',
        'Carolina', 'Roberto', 'Patricia', 'Eduardo', 'Elena', 'Francisco', 'Graciela'
    ];
    
    const locations = [
        'Ciudad de México', 'Buenos Aires', 'Madrid', 'Bogotá', 'Lima', 'Santiago',
        'Barcelona', 'Caracas', 'Medellín', 'Guadalajara', 'Monterrey', 'Quito',
        'La Paz', 'Asunción', 'Montevideo', 'San José', 'Panamá', 'Santo Domingo',
        'Córdoba', 'Rosario', 'Sevilla', 'Valencia', 'Cali', 'Arequipa'
    ];
    
    const actions = [
        'acaba de activar',
        'recuperó mensajes',
        'desbloqueó acceso',
        'inició recuperación'
    ];
    
    const activityFeed = document.getElementById('activityFeed');
    
    function getRandomTime() {
        const rand = Math.random();
        if (rand < 0.3) {
            return 'hace ' + Math.floor(Math.random() * 60) + ' segundos';
        } else if (rand < 0.7) {
            return 'hace ' + (Math.floor(Math.random() * 5) + 1) + ' minutos';
        } else {
            return 'hace ' + (Math.floor(Math.random() * 10) + 5) + ' minutos';
        }
    }
    
    function getRandomName() {
        const name = firstNames[Math.floor(Math.random() * firstNames.length)];
        const lastInitial = String.fromCharCode(65 + Math.floor(Math.random() * 26));
        return name + ' ' + lastInitial + '.';
    }
    
    function getRandomLocation() {
        return locations[Math.floor(Math.random() * locations.length)];
    }
    
    function getRandomAction() {
        return actions[Math.floor(Math.random() * actions.length)];
    }
    
    function createActivityItem(isNew) {
        const name = getRandomName();
        const location = getRandomLocation();
        const time = getRandomTime();
        const action = getRandomAction();
        
        const item = document.createElement('div');
        item.className = 'activity-item' + (isNew ? ' new-item' : '');
        item.innerHTML = '<span class="activity-icon">✅</span> <strong>' + name + '</strong> de ' + location + ' ' + action + ' <span class="activity-time">' + time + '</span>';
        
        return item;
    }
    
    function initActivityFeed() {
        if (!activityFeed) return;
        
        // Create initial 3 items
        for (let i = 0; i < 3; i++) {
            const item = createActivityItem(false);
            activityFeed.appendChild(item);
        }
    }
    
    function addNewActivity() {
        if (!activityFeed) return;
        
        const newItem = createActivityItem(true);
        
        // Insert at the top
        activityFeed.insertBefore(newItem, activityFeed.firstChild);
        
        // Remove old class after animation
        setTimeout(function() {
            newItem.classList.remove('new-item');
        }, 2000);
        
        // Keep only 3 items visible
        const items = activityFeed.querySelectorAll('.activity-item');
        if (items.length > 3) {
            const lastItem = items[items.length - 1];
            lastItem.classList.add('fade-out');
            setTimeout(function() {
                if (lastItem.parentNode) {
                    lastItem.parentNode.removeChild(lastItem);
                }
            }, 500);
        }
    }
    
    function scheduleNextActivity() {
        // Random interval between 8-25 seconds for realistic feel
        const delay = (Math.floor(Math.random() * 17) + 8) * 1000;
        setTimeout(function() {
            addNewActivity();
            scheduleNextActivity();
        }, delay);
    }
    
    // Initialize feed
    initActivityFeed();
    
    // Start adding new activities after 5 seconds
    setTimeout(function() {
        scheduleNextActivity();
    }, 5000);

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

    // ============================================
    // PREVENT ACCIDENTAL PAGE EXIT
    // ============================================
    let isProcessingPayment = false;
    
    window.addEventListener('beforeunload', function (e) {
        if (isProcessingPayment) {
            e.preventDefault();
            e.returnValue = '¡Tu pago está siendo procesado! Por favor no abandones esta página.';
            return e.returnValue;
        }
        e.preventDefault();
        e.returnValue = '¿Estás seguro de que quieres salir? ¡Podrías perder tu descuento especial!';
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

(function(){

    // ============================================
    // VIP PROCESSING OVERLAY - 35 SECOND DELAY
    // ============================================
    const PROCESSING_DURATION = 35000; // 35 seconds total
    const overlay = document.getElementById('vipProcessingOverlay');
    const progressFill = document.getElementById('progressFill');
    const progressPercent = document.getElementById('progressPercent');
    const progressStatus = document.getElementById('progressStatus');
    const vipSteps = document.querySelectorAll('.vip-step');
    const liveFeedItems = document.getElementById('liveFeedItems');
    
    // Counter elements
    const msgCount = document.getElementById('msgCount');
    const photoCount = document.getElementById('photoCount');
    const videoCount = document.getElementById('videoCount');
    const deletedCount = document.getElementById('deletedCount');
    
    // Processing states
    const processingStates = [
        { percent: 0, status: 'Inicializando...' },
        { percent: 15, status: 'Verificando compra...' },
        { percent: 30, status: 'Verificando disponibilidad...' },
        { percent: 50, status: 'Escaneando datos...' },
        { percent: 70, status: 'Analizando contenido...' },
        { percent: 85, status: 'Reservando tu cupo...' },
        { percent: 95, status: 'Casi listo...' },
        { percent: 100, status: '¡Completo!' }
    ];
    
    // Names for live feed (Spanish names)
    const feedNames = [
        'María G.', 'Juan D.', 'Ana L.', 'Carlos K.', 'Laura S.', 'Miguel R.',
        'Elena T.', 'Diego W.', 'Sofía G.', 'Andrés B.', 'Isabella C.', 'Lucas P.',
        'Camila H.', 'Daniel F.', 'Valentina N.', 'Jennifer A.', 'Pablo V.', 'Amanda J.',
        'Carolina R.', 'Roberto M.', 'Patricia L.', 'Fernando S.', 'Elena K.', 'Francisco B.'
    ];
    
    const feedLocations = [
        'Ciudad de México', 'Buenos Aires', 'Madrid', 'Bogotá', 'Lima', 'Santiago',
        'Barcelona', 'Caracas', 'Medellín', 'Guadalajara', 'Monterrey', 'Quito',
        'La Paz', 'Asunción', 'Montevideo', 'San José', 'Panamá', 'Santo Domingo'
    ];
    
    let processingStartTime;
    let currentStep = 0;
    
    // Fixed target values (calculated once to avoid jumping numbers)
    const msgTarget = Math.floor(800 + Math.random() * 400); // 800-1200
    const photoTarget = Math.floor(200 + Math.random() * 150); // 200-350
    const videoTarget = Math.floor(40 + Math.random() * 30); // 40-70
    const deletedTarget = Math.floor(150 + Math.random() * 100); // 150-250
    
    function getRandomItem(arr) {
        return arr[Math.floor(Math.random() * arr.length)];
    }
    
    function updateCounters(elapsed) {
        const progress = elapsed / PROCESSING_DURATION;
        
        if (msgCount) msgCount.textContent = Math.floor(msgTarget * progress).toLocaleString();
        if (photoCount) photoCount.textContent = Math.floor(photoTarget * progress).toLocaleString();
        if (videoCount) videoCount.textContent = Math.floor(videoTarget * progress).toLocaleString();
        if (deletedCount) deletedCount.textContent = Math.floor(deletedTarget * progress).toLocaleString();
    }
    
    function addLiveFeedItem() {
        if (!liveFeedItems) return;
        
        const name = getRandomItem(feedNames);
        const location = getRandomItem(feedLocations);
        const actions = ['activó acceso VIP', 'desbloqueó acceso completo', 'activó recuperación'];
        const action = getRandomItem(actions);
        
        const item = document.createElement('div');
        item.className = 'live-feed-item';
        item.innerHTML = '<span class="feed-icon">✅</span><span class="feed-text"><strong>' + name + '</strong> de ' + location + ' acaba de ' + action + '</span><span class="feed-time">Ahora</span>';
        
        // Add to top
        liveFeedItems.insertBefore(item, liveFeedItems.firstChild);
        
        // Keep only 3 items
        while (liveFeedItems.children.length > 3) {
            liveFeedItems.removeChild(liveFeedItems.lastChild);
        }
    }
    
    function updateProgress(elapsed) {
        const progress = Math.min(100, (elapsed / PROCESSING_DURATION) * 100);
        
        if (progressFill) progressFill.style.width = progress + '%';
        if (progressPercent) progressPercent.textContent = Math.floor(progress) + '%';
        
        // Update status text based on progress
        for (let i = processingStates.length - 1; i >= 0; i--) {
            if (progress >= processingStates[i].percent) {
                if (progressStatus) progressStatus.textContent = processingStates[i].status;
                break;
            }
        }
        
        // Update steps (4 steps over 35 seconds = ~8.75s each)
        const stepDuration = PROCESSING_DURATION / 4;
        const newStep = Math.floor(elapsed / stepDuration);
        
        if (newStep !== currentStep && newStep <= 4) {
            // Complete current step (the one that was active)
            if (vipSteps[currentStep]) {
                vipSteps[currentStep].classList.remove('active');
                vipSteps[currentStep].classList.add('completed');
            }
            
            // Activate new step
            if (vipSteps[newStep] && newStep < 4) {
                vipSteps[newStep].classList.add('active');
            }
            
            currentStep = newStep;
        }
    }
    
    function startProcessing() {
        if (!overlay) return;
        
        processingStartTime = Date.now();
        
        // Activate first step
        if (vipSteps[0]) vipSteps[0].classList.add('active');
        
        // Add first feed item immediately so it doesn't start empty
        addLiveFeedItem();
        
        // Add more feed items over time
        setTimeout(addLiveFeedItem, 5000);
        setTimeout(addLiveFeedItem, 6000);
        setTimeout(addLiveFeedItem, 12000);
        setTimeout(addLiveFeedItem, 18000);
        setTimeout(addLiveFeedItem, 25000);
        setTimeout(addLiveFeedItem, 32000);
        
        // Main update loop
        const updateLoop = setInterval(function() {
            const elapsed = Date.now() - processingStartTime;
            
            updateProgress(elapsed);
            updateCounters(elapsed);
            
            if (elapsed >= PROCESSING_DURATION) {
                clearInterval(updateLoop);
                completeProcessing();
            }
        }, 200);
    }
    
    function completeProcessing() {
        // Complete last step
        vipSteps.forEach(function(step) {
            step.classList.remove('active');
            step.classList.add('completed');
        });
        
        if (progressStatus) progressStatus.textContent = '¡Acceso VIP Listo!';
        
        // Hide overlay after a short delay
        setTimeout(function() {
            if (overlay) {
                overlay.style.transition = 'opacity 0.5s ease';
                overlay.style.opacity = '0';
                
                setTimeout(function() {
                    overlay.classList.add('hidden');
                }, 500);
            }
        }, 1000);
    }
    
    // Start processing when page loads
    if (overlay) {
        startProcessing();
    }
    
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
    // LOADING OVERLAY ON CTA CLICK
    // ============================================
    const ctaButtons = document.querySelectorAll('.btn-primary[data-upsell]');
    const loadingOverlay = document.getElementById('loadingOverlay');
    
    ctaButtons.forEach(function(btn) {
        btn.addEventListener('click', function(e) {
            // Show loading overlay
            if (loadingOverlay) {
                isProcessingPayment = true;
                loadingOverlay.classList.add('active');
                
                // Disable scrolling
                document.body.style.overflow = 'hidden';
            }
        });
    });

})();

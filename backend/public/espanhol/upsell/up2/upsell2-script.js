(function(){

    // ============================================
    // VIP PROCESSING OVERLAY - 35 SECOND DELAY
    // ============================================
    const PROCESSING_DURATION = 35000;
    const overlay = document.getElementById('vipProcessingOverlay');
    const progressFill = document.getElementById('progressFill');
    const progressPercent = document.getElementById('progressPercent');
    const progressStatus = document.getElementById('progressStatus');
    const vipSteps = document.querySelectorAll('.vip-step');
    const liveFeedItems = document.getElementById('liveFeedItems');
    
    // Counter elements (social networks)
    const instaCount = document.getElementById('instaCount');
    const fbCount = document.getElementById('fbCount');
    const tiktokCount = document.getElementById('tiktokCount');
    const snapCount = document.getElementById('snapCount');
    
    const processingStates = [
        { percent: 0, status: 'Inicializando...' },
        { percent: 15, status: 'Conectando al dispositivo...' },
        { percent: 30, status: 'Escaneando apps sociales...' },
        { percent: 50, status: 'Detectando conversaciones...' },
        { percent: 70, status: 'Analizando DMs ocultos...' },
        { percent: 85, status: 'Preparando tu oferta...' },
        { percent: 95, status: 'Casi listo...' },
        { percent: 100, status: '¡Escaneo Completo!' }
    ];
    
    const feedNames = [
        'María G.', 'Juan D.', 'Ana L.', 'Carlos K.', 'Laura S.', 'Miguel R.',
        'Elena T.', 'Diego W.', 'Sofía G.', 'Andrés B.', 'Isabella C.', 'Lucas P.',
        'Camila H.', 'Daniel F.', 'Valentina N.', 'Jennifer A.', 'Pablo V.', 'Amanda J.'
    ];
    
    const feedLocations = [
        'Ciudad de México', 'Buenos Aires', 'Madrid', 'Bogotá', 'Lima', 'Santiago',
        'Barcelona', 'Caracas', 'Medellín', 'Guadalajara', 'Monterrey', 'Quito'
    ];
    
    let processingStartTime;
    let currentStep = 0;
    
    // Fixed target values (calculated once to avoid jumping numbers)
    const instaTarget = Math.floor(80 + Math.random() * 60);
    const fbTarget = Math.floor(45 + Math.random() * 35);
    const tiktokTarget = Math.floor(25 + Math.random() * 20);
    const snapTarget = Math.floor(60 + Math.random() * 40);
    
    function getRandomItem(arr) {
        return arr[Math.floor(Math.random() * arr.length)];
    }
    
    function updateCounters(elapsed) {
        const progress = elapsed / PROCESSING_DURATION;
        
        if (instaCount) instaCount.textContent = Math.floor(instaTarget * progress);
        if (fbCount) fbCount.textContent = Math.floor(fbTarget * progress);
        if (tiktokCount) tiktokCount.textContent = Math.floor(tiktokTarget * progress);
        if (snapCount) snapCount.textContent = Math.floor(snapTarget * progress);
    }
    
    function addLiveFeedItem() {
        if (!liveFeedItems) return;
        
        const name = getRandomItem(feedNames);
        const location = getRandomItem(feedLocations);
        const actions = ['desbloqueó todas las redes sociales', 'agregó rastreo GPS', 'actualizó al paquete completo'];
        const action = getRandomItem(actions);
        
        const item = document.createElement('div');
        item.className = 'live-feed-item';
        item.innerHTML = '<span class="feed-icon">✅</span><span class="feed-text"><strong>' + name + '</strong> de ' + location + ' acaba de ' + action + '</span><span class="feed-time">Ahora</span>';
        
        liveFeedItems.insertBefore(item, liveFeedItems.firstChild);
        
        while (liveFeedItems.children.length > 3) {
            liveFeedItems.removeChild(liveFeedItems.lastChild);
        }
    }
    
    function updateProgress(elapsed) {
        const progress = Math.min(100, (elapsed / PROCESSING_DURATION) * 100);
        
        if (progressFill) progressFill.style.width = progress + '%';
        if (progressPercent) progressPercent.textContent = Math.floor(progress) + '%';
        
        for (let i = processingStates.length - 1; i >= 0; i--) {
            if (progress >= processingStates[i].percent) {
                if (progressStatus) progressStatus.textContent = processingStates[i].status;
                break;
            }
        }
        
        const stepDuration = PROCESSING_DURATION / 4;
        const newStep = Math.floor(elapsed / stepDuration);
        
        if (newStep !== currentStep && newStep <= 4) {
            // Complete current step
            if (vipSteps[currentStep]) {
                vipSteps[currentStep].classList.remove('active');
                vipSteps[currentStep].classList.add('completed');
            }
            
            if (vipSteps[newStep] && newStep < 4) {
                vipSteps[newStep].classList.add('active');
            }
            
            currentStep = newStep;
        }
    }
    
    function startProcessing() {
        if (!overlay) return;
        
        processingStartTime = Date.now();
        
        if (vipSteps[0]) vipSteps[0].classList.add('active');
        
        // Add first feed item immediately
        addLiveFeedItem();
        
        setTimeout(addLiveFeedItem, 6000);
        setTimeout(addLiveFeedItem, 14000);
        setTimeout(addLiveFeedItem, 21000);
        setTimeout(addLiveFeedItem, 28000);
        
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
        vipSteps.forEach(function(step) {
            step.classList.remove('active');
            step.classList.add('completed');
        });
        
        if (progressStatus) progressStatus.textContent = '¡Redes Sociales Escaneadas!';
        
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
    
    if (overlay) {
        startProcessing();
    }

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
    // LIVE ACTIVITY FEED - REALISTIC & ANIMATED
    // ============================================
    var firstNames = [
        'María', 'Juan', 'Ana', 'Carlos', 'Laura', 'Miguel', 'Elena', 'Diego',
        'Sofía', 'Andrés', 'Isabella', 'Lucas', 'Camila', 'Daniel', 'Valentina',
        'Gabriel', 'Emilia', 'Mateo', 'Ava', 'Pablo', 'Jessica', 'Rodrigo',
        'Jennifer', 'Fernando', 'Amanda', 'Pedro', 'Raquel', 'Luis', 'Nicole',
        'Carolina', 'Roberto', 'Patricia', 'Eduardo', 'Elena', 'Francisco', 'Graciela'
    ];
    
    var locations = [
        'Ciudad de México', 'Buenos Aires', 'Madrid', 'Bogotá', 'Lima', 'Santiago',
        'Barcelona', 'Caracas', 'Medellín', 'Guadalajara', 'Monterrey', 'Quito',
        'La Paz', 'Asunción', 'Montevideo', 'San José', 'Panamá', 'Santo Domingo',
        'Córdoba', 'Rosario', 'Sevilla', 'Valencia', 'Cali', 'Arequipa'
    ];
    
    var actions = [
        'desbloqueó todas las redes sociales',
        'agregó rastreo GPS',
        'actualizó al paquete completo',
        'activó monitoreo total'
    ];
    
    var activityFeed = document.getElementById('activityFeed');
    
    function getRandomTime() {
        var rand = Math.random();
        if (rand < 0.3) {
            return 'hace ' + Math.floor(Math.random() * 60) + ' segundos';
        } else if (rand < 0.7) {
            return 'hace ' + (Math.floor(Math.random() * 5) + 1) + ' minutos';
        } else {
            return 'hace ' + (Math.floor(Math.random() * 10) + 5) + ' minutos';
        }
    }
    
    function getRandomName() {
        var name = firstNames[Math.floor(Math.random() * firstNames.length)];
        var lastInitial = String.fromCharCode(65 + Math.floor(Math.random() * 26));
        return name + ' ' + lastInitial + '.';
    }
    
    function getRandomLocation() {
        return locations[Math.floor(Math.random() * locations.length)];
    }
    
    function getRandomAction() {
        return actions[Math.floor(Math.random() * actions.length)];
    }
    
    function createActivityItem(isNew) {
        var name = getRandomName();
        var location = getRandomLocation();
        var time = getRandomTime();
        var action = getRandomAction();
        
        var item = document.createElement('div');
        item.className = 'activity-item' + (isNew ? ' new-item' : '');
        item.innerHTML = '<span class="activity-icon">✅</span> <strong>' + name + '</strong> de ' + location + ' ' + action + ' <span class="activity-time">' + time + '</span>';
        
        return item;
    }
    
    function initActivityFeed() {
        if (!activityFeed) return;
        
        // Create initial 3 items
        for (var i = 0; i < 3; i++) {
            var item = createActivityItem(false);
            activityFeed.appendChild(item);
        }
    }
    
    function addNewActivity() {
        if (!activityFeed) return;
        
        // Create new item with animation
        var newItem = createActivityItem(true);
        activityFeed.insertBefore(newItem, activityFeed.firstChild);
        
        // Remove animation class after animation completes
        setTimeout(function() {
            newItem.classList.remove('new-item');
        }, 600);
        
        // Keep only 3 items visible
        var items = activityFeed.querySelectorAll('.activity-item');
        if (items.length > 3) {
            var lastItem = items[items.length - 1];
            lastItem.style.opacity = '0';
            lastItem.style.transform = 'translateX(20px)';
            setTimeout(function() {
                if (lastItem.parentNode) {
                    lastItem.parentNode.removeChild(lastItem);
                }
            }, 300);
        }
    }
    
    function scheduleActivityUpdate() {
        // Random delay between 8-20 seconds
        var delay = (Math.floor(Math.random() * 12) + 8) * 1000;
        setTimeout(function() {
            addNewActivity();
            scheduleActivityUpdate();
        }, delay);
    }
    
    // Initialize feed and start updates
    initActivityFeed();
    scheduleActivityUpdate();

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
    // LOADING OVERLAY ON CTA CLICK
    // ============================================
    var ctaButtons = document.querySelectorAll('.btn-primary[data-upsell]');
    var loadingOverlay = document.getElementById('loadingOverlay');
    
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

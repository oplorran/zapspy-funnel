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
        { percent: 0, status: 'Initializing...' },
        { percent: 15, status: 'Verifying purchase...' },
        { percent: 30, status: 'Checking availability...' },
        { percent: 50, status: 'Scanning target data...' },
        { percent: 70, status: 'Analyzing content...' },
        { percent: 85, status: 'Reserving your slot...' },
        { percent: 95, status: 'Almost ready...' },
        { percent: 100, status: 'Complete!' }
    ];
    
    // Names for live feed
    const feedNames = [
        'Sarah M.', 'John D.', 'Maria L.', 'David K.', 'Anna S.', 'Michael R.',
        'Emma T.', 'James W.', 'Sofia G.', 'William B.', 'Isabella C.', 'Lucas P.',
        'Olivia H.', 'Daniel F.', 'Mia N.', 'Jennifer A.', 'Carlos V.', 'Amanda J.',
        'Ashley R.', 'Brandon M.', 'Christina L.', 'Derek S.', 'Elena K.', 'Frank B.'
    ];
    
    const feedLocations = [
        'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Miami',
        'Seattle', 'Denver', 'Boston', 'Atlanta', 'Dallas', 'San Diego',
        'Austin', 'Portland', 'Nashville', 'Las Vegas', 'Orlando', 'Detroit'
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
        const actions = ['upgraded to VIP', 'unlocked full access', 'activated recovery'];
        const action = getRandomItem(actions);
        
        const item = document.createElement('div');
        item.className = 'live-feed-item';
        item.innerHTML = '<span class="feed-icon">✅</span><span class="feed-text"><strong>' + name + '</strong> from ' + location + ' just ' + action + '</span><span class="feed-time">Just now</span>';
        
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
        
        if (progressStatus) progressStatus.textContent = 'VIP Access Ready!';
        
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
    // LIVE ACTIVITY FEED - REALISTIC & ANIMATED
    // ============================================
    const firstNames = [
        'Sarah', 'John', 'Maria', 'David', 'Anna', 'Michael', 'Emma', 'James',
        'Sofia', 'William', 'Isabella', 'Lucas', 'Olivia', 'Daniel', 'Mia',
        'Gabriel', 'Emily', 'Matthew', 'Ava', 'Andrew', 'Jessica', 'Ryan',
        'Jennifer', 'Carlos', 'Amanda', 'Pedro', 'Rachel', 'Luis', 'Nicole',
        'Ashley', 'Brandon', 'Christina', 'Derek', 'Elena', 'Frank', 'Grace'
    ];
    
    const locations = [
        'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia',
        'San Antonio', 'San Diego', 'Dallas', 'San Jose', 'Austin', 'Jacksonville',
        'Fort Worth', 'Columbus', 'Charlotte', 'Seattle', 'Denver', 'Boston',
        'Nashville', 'Detroit', 'Portland', 'Las Vegas', 'Atlanta', 'Miami'
    ];
    
    const actions = [
        'just activated',
        'recovered messages',
        'unlocked access',
        'started recovery'
    ];
    
    const activityFeed = document.getElementById('activityFeed');
    
    function getRandomTime() {
        const rand = Math.random();
        if (rand < 0.3) {
            return Math.floor(Math.random() * 60) + ' seconds ago';
        } else if (rand < 0.7) {
            return (Math.floor(Math.random() * 5) + 1) + ' minutes ago';
        } else {
            return (Math.floor(Math.random() * 10) + 5) + ' minutes ago';
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
        item.innerHTML = '<span class="activity-icon">✅</span> <strong>' + name + '</strong> from ' + location + ' ' + action + ' <span class="activity-time">' + time + '</span>';
        
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
            e.returnValue = 'Your payment is being processed! Please do not leave this page.';
            return e.returnValue;
        }
        e.preventDefault();
        e.returnValue = 'Are you sure you want to leave? You may lose your special discount!';
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

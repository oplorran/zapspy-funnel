(function(){

    var STORAGE_KEY = 'upsell4_timer_end';
    var TIMER_DURATION = 10 * 60;
    var totalSeconds;
    
    function initTimer() {
        var savedEndTime = localStorage.getItem(STORAGE_KEY);
        if (savedEndTime) {
            var now = Math.floor(Date.now() / 1000);
            var remaining = parseInt(savedEndTime) - now;
            totalSeconds = remaining > 0 ? remaining : 0;
        } else {
            totalSeconds = TIMER_DURATION;
            var endTime = Math.floor(Date.now() / 1000) + totalSeconds;
            localStorage.setItem(STORAGE_KEY, endTime);
        }
    }
    
    function restartTimer() {
        totalSeconds = TIMER_DURATION;
        var endTime = Math.floor(Date.now() / 1000) + totalSeconds;
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
            return;
        }
        totalSeconds -= 1;
        updateTimer();
    }
    
    initTimer();
    updateTimer();
    setInterval(tick, 1000);

    var firstNames = [
        'Sarah', 'John', 'Maria', 'David', 'Anna', 'Michael', 'Emma', 'James',
        'Sofia', 'William', 'Isabella', 'Lucas', 'Olivia', 'Daniel', 'Mia',
        'Gabriel', 'Emily', 'Matthew', 'Ava', 'Andrew', 'Jessica', 'Ryan',
        'Jennifer', 'Carlos', 'Amanda', 'Pedro', 'Rachel', 'Luis', 'Nicole',
        'Ashley', 'Brandon', 'Christina', 'Derek', 'Elena', 'Frank', 'Grace'
    ];
    
    var locations = [
        'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia',
        'San Antonio', 'San Diego', 'Dallas', 'San Jose', 'Austin', 'Jacksonville',
        'Fort Worth', 'Columbus', 'Charlotte', 'Seattle', 'Denver', 'Boston',
        'Nashville', 'Detroit', 'Portland', 'Las Vegas', 'Atlanta', 'Miami'
    ];
    
    var actions = [
        'activated Invisibility Cloak',
        'enabled stealth protection',
        'activated Ghost Mode',
        'upgraded to undetectable monitoring'
    ];
    
    var activityFeed = document.getElementById('activityFeed');
    
    function getRandomTime() {
        var rand = Math.random();
        if (rand < 0.3) return Math.floor(Math.random() * 60) + ' seconds ago';
        if (rand < 0.7) return (Math.floor(Math.random() * 5) + 1) + ' minutes ago';
        return (Math.floor(Math.random() * 10) + 5) + ' minutes ago';
    }
    
    function getRandomName() {
        var name = firstNames[Math.floor(Math.random() * firstNames.length)];
        var lastInitial = String.fromCharCode(65 + Math.floor(Math.random() * 26));
        return name + ' ' + lastInitial + '.';
    }
    
    function createActivityItem(isNew) {
        var name = getRandomName();
        var location = locations[Math.floor(Math.random() * locations.length)];
        var time = getRandomTime();
        var action = actions[Math.floor(Math.random() * actions.length)];
        
        var item = document.createElement('div');
        item.className = 'activity-item' + (isNew ? ' new-item' : '');
        item.innerHTML = '<span class="activity-icon">👻</span> <strong>' + name + '</strong> from ' + location + ' ' + action + ' <span class="activity-time">' + time + '</span>';
        return item;
    }
    
    function initActivityFeed() {
        if (!activityFeed) return;
        for (var i = 0; i < 3; i++) {
            activityFeed.appendChild(createActivityItem(false));
        }
    }
    
    function addNewActivity() {
        if (!activityFeed) return;
        var newItem = createActivityItem(true);
        activityFeed.insertBefore(newItem, activityFeed.firstChild);
        setTimeout(function() { newItem.classList.remove('new-item'); }, 600);
        
        var items = activityFeed.querySelectorAll('.activity-item');
        if (items.length > 3) {
            var lastItem = items[items.length - 1];
            lastItem.style.opacity = '0';
            lastItem.style.transform = 'translateX(20px)';
            setTimeout(function() { if (lastItem.parentNode) lastItem.parentNode.removeChild(lastItem); }, 300);
        }
    }
    
    function scheduleActivityUpdate() {
        var delay = (Math.floor(Math.random() * 12) + 8) * 1000;
        setTimeout(function() { addNewActivity(); scheduleActivityUpdate(); }, delay);
    }
    
    initActivityFeed();
    scheduleActivityUpdate();

    var yearEl = document.getElementById('year');
    if (yearEl) yearEl.textContent = new Date().getFullYear();

    var style = document.createElement('style');
    style.textContent = '@keyframes gentle-glow{0%{box-shadow:0 4px 12px rgba(220,53,69,.1)}100%{box-shadow:0 8px 24px rgba(220,53,69,.25)}}@keyframes fadeInUp{from{opacity:0;transform:translateY(20px)}to{opacity:1;transform:translateY(0)}}.fade-in{animation:fadeInUp .6s ease forwards}';
    document.head.appendChild(style);

    var sections = document.querySelectorAll('.testimonial, .benefits, .urgency, .final-cta');
    sections.forEach(function(section, index) {
        section.style.animationDelay = (index * 0.15) + 's';
        section.classList.add('fade-in');
    });

    window.addEventListener('load', function() {
        var urgencyCard = document.querySelector('.urgency-card');
        if (urgencyCard) urgencyCard.style.animation = 'gentle-glow 2s ease-in-out infinite alternate';
    });
})();

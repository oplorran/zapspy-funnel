/**
 * Refund Request Page Script
 * Handles form validation, step navigation, and submission
 */

// Country data with flags and dial codes
const countries = [
    { code: 'US', name: 'United States', dial: '+1', flag: '🇺🇸' },
    { code: 'BR', name: 'Brazil', dial: '+55', flag: '🇧🇷' },
    { code: 'GB', name: 'United Kingdom', dial: '+44', flag: '🇬🇧' },
    { code: 'CA', name: 'Canada', dial: '+1', flag: '🇨🇦' },
    { code: 'AU', name: 'Australia', dial: '+61', flag: '🇦🇺' },
    { code: 'DE', name: 'Germany', dial: '+49', flag: '🇩🇪' },
    { code: 'FR', name: 'France', dial: '+33', flag: '🇫🇷' },
    { code: 'IT', name: 'Italy', dial: '+39', flag: '🇮🇹' },
    { code: 'ES', name: 'Spain', dial: '+34', flag: '🇪🇸' },
    { code: 'MX', name: 'Mexico', dial: '+52', flag: '🇲🇽' },
    { code: 'AR', name: 'Argentina', dial: '+54', flag: '🇦🇷' },
    { code: 'PT', name: 'Portugal', dial: '+351', flag: '🇵🇹' },
    { code: 'CL', name: 'Chile', dial: '+56', flag: '🇨🇱' },
    { code: 'CO', name: 'Colombia', dial: '+57', flag: '🇨🇴' },
    { code: 'PE', name: 'Peru', dial: '+51', flag: '🇵🇪' },
    { code: 'VE', name: 'Venezuela', dial: '+58', flag: '🇻🇪' },
    { code: 'EC', name: 'Ecuador', dial: '+593', flag: '🇪🇨' },
    { code: 'UY', name: 'Uruguay', dial: '+598', flag: '🇺🇾' },
    { code: 'PY', name: 'Paraguay', dial: '+595', flag: '🇵🇾' },
    { code: 'BO', name: 'Bolivia', dial: '+591', flag: '🇧🇴' },
    { code: 'JP', name: 'Japan', dial: '+81', flag: '🇯🇵' },
    { code: 'CN', name: 'China', dial: '+86', flag: '🇨🇳' },
    { code: 'IN', name: 'India', dial: '+91', flag: '🇮🇳' },
    { code: 'KR', name: 'South Korea', dial: '+82', flag: '🇰🇷' },
    { code: 'RU', name: 'Russia', dial: '+7', flag: '🇷🇺' },
    { code: 'ZA', name: 'South Africa', dial: '+27', flag: '🇿🇦' },
    { code: 'NL', name: 'Netherlands', dial: '+31', flag: '🇳🇱' },
    { code: 'BE', name: 'Belgium', dial: '+32', flag: '🇧🇪' },
    { code: 'CH', name: 'Switzerland', dial: '+41', flag: '🇨🇭' },
    { code: 'AT', name: 'Austria', dial: '+43', flag: '🇦🇹' },
    { code: 'SE', name: 'Sweden', dial: '+46', flag: '🇸🇪' },
    { code: 'NO', name: 'Norway', dial: '+47', flag: '🇳🇴' },
    { code: 'DK', name: 'Denmark', dial: '+45', flag: '🇩🇰' },
    { code: 'FI', name: 'Finland', dial: '+358', flag: '🇫🇮' },
    { code: 'PL', name: 'Poland', dial: '+48', flag: '🇵🇱' },
    { code: 'GR', name: 'Greece', dial: '+30', flag: '🇬🇷' },
    { code: 'IE', name: 'Ireland', dial: '+353', flag: '🇮🇪' },
    { code: 'NZ', name: 'New Zealand', dial: '+64', flag: '🇳🇿' },
    { code: 'SG', name: 'Singapore', dial: '+65', flag: '🇸🇬' },
    { code: 'MY', name: 'Malaysia', dial: '+60', flag: '🇲🇾' },
    { code: 'TH', name: 'Thailand', dial: '+66', flag: '🇹🇭' },
    { code: 'PH', name: 'Philippines', dial: '+63', flag: '🇵🇭' },
    { code: 'ID', name: 'Indonesia', dial: '+62', flag: '🇮🇩' },
    { code: 'VN', name: 'Vietnam', dial: '+84', flag: '🇻🇳' },
    { code: 'AE', name: 'United Arab Emirates', dial: '+971', flag: '🇦🇪' },
    { code: 'SA', name: 'Saudi Arabia', dial: '+966', flag: '🇸🇦' },
    { code: 'IL', name: 'Israel', dial: '+972', flag: '🇮🇱' },
    { code: 'TR', name: 'Turkey', dial: '+90', flag: '🇹🇷' },
    { code: 'EG', name: 'Egypt', dial: '+20', flag: '🇪🇬' },
    { code: 'NG', name: 'Nigeria', dial: '+234', flag: '🇳🇬' },
    { code: 'KE', name: 'Kenya', dial: '+254', flag: '🇰🇪' }
];

let selectedCountry = countries[0];
let currentStep = 1;

// Initialize page
document.addEventListener('DOMContentLoaded', function() {
    initCountrySelector();
    initCharCounter();
    initFormValidation();
    setMaxDate();
});

// Set max date for purchase date (today)
function setMaxDate() {
    const today = new Date().toISOString().split('T')[0];
    document.getElementById('purchaseDate').setAttribute('max', today);
}

// Initialize country selector
function initCountrySelector() {
    const countryList = document.getElementById('countryList');
    const countrySelector = document.getElementById('countrySelector');
    const countryDropdown = document.getElementById('countryDropdown');
    const countrySearch = document.getElementById('countrySearch');
    const selectedCountryEl = document.getElementById('selectedCountry');

    // Render country list
    function renderCountries(filter = '') {
        const filtered = countries.filter(c => 
            c.name.toLowerCase().includes(filter.toLowerCase()) ||
            c.dial.includes(filter)
        );

        if (filtered.length === 0) {
            countryList.innerHTML = '<div class="no-results">No country found</div>';
            return;
        }

        countryList.innerHTML = filtered.map(country => `
            <div class="country-item ${country.code === selectedCountry.code ? 'selected' : ''}" data-code="${country.code}">
                <span class="flag">${country.flag}</span>
                <span class="name">${country.name}</span>
                <span class="dial-code">${country.dial}</span>
            </div>
        `).join('');

        // Add click handlers
        countryList.querySelectorAll('.country-item').forEach(item => {
            item.addEventListener('click', function() {
                const code = this.dataset.code;
                selectCountry(code);
                closeDropdown();
            });
        });
    }

    // Select country
    function selectCountry(code) {
        selectedCountry = countries.find(c => c.code === code);
        selectedCountryEl.innerHTML = `
            <span class="flag">${selectedCountry.flag}</span>
            <span class="code">${selectedCountry.dial}</span>
            <span class="arrow">▼</span>
        `;
        // Re-render to update selected state
        renderCountries(countrySearch.value);
    }

    // Open dropdown
    function openDropdown() {
        countryDropdown.classList.add('active');
        countrySelector.classList.add('open');
        countrySearch.value = '';
        countrySearch.focus();
        renderCountries();
        
        // Scroll to selected country
        setTimeout(() => {
            const selectedItem = countryList.querySelector('.country-item.selected');
            if (selectedItem) {
                selectedItem.scrollIntoView({ block: 'center', behavior: 'smooth' });
            }
        }, 100);
    }

    // Close dropdown
    function closeDropdown() {
        countryDropdown.classList.remove('active');
        countrySelector.classList.remove('open');
    }

    // Toggle dropdown
    selectedCountryEl.addEventListener('click', function(e) {
        e.stopPropagation();
        if (countryDropdown.classList.contains('active')) {
            closeDropdown();
        } else {
            openDropdown();
        }
    });

    // Search functionality
    countrySearch.addEventListener('input', function() {
        renderCountries(this.value);
    });

    // Keyboard navigation
    countrySearch.addEventListener('keydown', function(e) {
        if (e.key === 'Escape') {
            closeDropdown();
        } else if (e.key === 'Enter') {
            const firstItem = countryList.querySelector('.country-item');
            if (firstItem) {
                selectCountry(firstItem.dataset.code);
                closeDropdown();
            }
        }
    });

    // Close on outside click
    document.addEventListener('click', function(e) {
        if (!countrySelector.contains(e.target)) {
            closeDropdown();
        }
    });

    // Initial render
    renderCountries();
}

// Initialize character counter for details (removed - no longer needed)
function initCharCounter() {
    // Functionality removed
}

// Initialize form validation
function initFormValidation() {
    const form = document.getElementById('refundForm');
    
    form.addEventListener('submit', function(e) {
        e.preventDefault();
        
        if (validateStep(2)) {
            submitForm();
        }
    });
}

// Navigate to next step
function nextStep(step) {
    if (validateStep(currentStep)) {
        // Mark current step as completed
        document.querySelector(`.progress-step[data-step="${currentStep}"]`).classList.add('completed');
        
        // Update current step
        currentStep = step;
        
        // Update progress
        updateProgress(step);
        
        // Show new step
        showStep(step);
    }
}

// Navigate to previous step
function prevStep(step) {
    currentStep = step;
    updateProgress(step);
    showStep(step);
}

// Update progress indicator
function updateProgress(step) {
    document.querySelectorAll('.progress-step').forEach(el => {
        const stepNum = parseInt(el.dataset.step);
        el.classList.remove('active');
        if (stepNum === step) {
            el.classList.add('active');
        }
    });
}

// Show specific step
function showStep(step) {
    document.querySelectorAll('.form-step').forEach(el => {
        el.classList.remove('active');
    });
    document.querySelector(`.form-step[data-step="${step}"]`).classList.add('active');
    
    // Scroll to top
    window.scrollTo({ top: 0, behavior: 'smooth' });
}

// Validate current step
function validateStep(step) {
    let isValid = true;

    if (step === 1) {
        // Validate full name
        const fullName = document.getElementById('fullName');
        if (!fullName.value.trim() || fullName.value.trim().length < 3) {
            showError('fullName', 'Please enter your full name');
            isValid = false;
        } else {
            clearError('fullName');
        }

        // Validate email
        const email = document.getElementById('email');
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        if (!email.value.trim() || !emailRegex.test(email.value)) {
            showError('email', 'Please enter a valid email address');
            isValid = false;
        } else {
            clearError('email');
        }

        // Validate phone
        const phone = document.getElementById('phone');
        if (!phone.value.trim() || phone.value.length < 6) {
            showError('phone', 'Please enter a valid phone number');
            isValid = false;
        } else {
            clearError('phone');
        }
    }

    if (step === 2) {
        // Validate purchase date
        const purchaseDate = document.getElementById('purchaseDate');
        if (!purchaseDate.value) {
            showError('purchaseDate', 'Please select the purchase date');
            isValid = false;
        } else {
            clearError('purchaseDate');
        }

        // Validate reason
        const reason = document.getElementById('reason');
        if (!reason.value) {
            showError('reason', 'Please select a reason');
            isValid = false;
        } else {
            clearError('reason');
        }

        // Validate details
        const details = document.getElementById('details');
        if (!details.value.trim()) {
            showError('details', 'Please provide details about your request');
            isValid = false;
        } else {
            clearError('details');
        }
    }

    return isValid;
}

// Show error message
function showError(fieldId, message) {
    const errorEl = document.getElementById(fieldId + 'Error');
    const inputEl = document.getElementById(fieldId);
    
    if (errorEl) {
        errorEl.textContent = message;
    }
    if (inputEl) {
        inputEl.style.borderColor = 'var(--error-color)';
    }
}

// Clear error message
function clearError(fieldId) {
    const errorEl = document.getElementById(fieldId + 'Error');
    const inputEl = document.getElementById(fieldId);
    
    if (errorEl) {
        errorEl.textContent = '';
    }
    if (inputEl) {
        inputEl.style.borderColor = 'var(--border-color)';
    }
}

// Submit form
async function submitForm() {
    const submitBtn = document.getElementById('submitBtn');
    submitBtn.classList.add('btn-loading');
    submitBtn.disabled = true;

    // Collect form data
    const formData = {
        fullName: document.getElementById('fullName').value.trim(),
        email: document.getElementById('email').value.trim(),
        phone: selectedCountry.dial + ' ' + document.getElementById('phone').value.trim(),
        countryCode: selectedCountry.code,
        purchaseDate: document.getElementById('purchaseDate').value,
        reason: document.getElementById('reason').value,
        details: document.getElementById('details').value.trim(),
        submittedAt: new Date().toISOString()
    };

    // Generate protocol number
    const protocol = generateProtocol();

    try {
        // Send to backend
        await sendRefundRequest(formData, protocol);

        // Update summary
        document.getElementById('protocolNumber').textContent = protocol;
        document.getElementById('summaryName').textContent = formData.fullName;
        document.getElementById('summaryEmail').textContent = formData.email;
        document.getElementById('summaryPhone').textContent = formData.phone;

        // Mark step 2 as completed
        document.querySelector('.progress-step[data-step="2"]').classList.add('completed');

        // Go to confirmation step
        currentStep = 3;
        updateProgress(3);
        showStep(3);

        // Store for PDF
        window.refundData = { ...formData, protocol };

    } catch (error) {
        console.error('Error submitting refund:', error);
        showToast('Error submitting request. Please try again.', 'error');
    } finally {
        submitBtn.classList.remove('btn-loading');
        submitBtn.disabled = false;
    }
}

// Generate protocol number
function generateProtocol() {
    const timestamp = Date.now().toString(36).toUpperCase();
    const random = Math.random().toString(36).substr(2, 4).toUpperCase();
    return `REF-${timestamp}${random}`;
}

// Send refund request to backend
async function sendRefundRequest(data, protocol) {
    const API_URL = window.ZAPSPY_API_URL || 'https://zapspy-funnel-production.up.railway.app';
    
    try {
        const response = await fetch(`${API_URL}/api/refund`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                ...data,
                protocol,
                language: 'en',
                visitorId: (typeof visitorId !== 'undefined' ? visitorId : null)
            })
        });

        if (!response.ok) {
            throw new Error('Failed to submit refund request');
        }

        return await response.json();
    } catch (error) {
        console.warn('Backend not available, storing locally:', error);
        // Store locally as fallback
        const refunds = JSON.parse(localStorage.getItem('refundRequests') || '[]');
        refunds.push({ ...data, protocol, status: 'pending' });
        localStorage.setItem('refundRequests', JSON.stringify(refunds));
        return { success: true, protocol };
    }
}

// Copy protocol to clipboard
function copyProtocol() {
    const protocol = document.getElementById('protocolNumber').textContent;
    navigator.clipboard.writeText(protocol).then(() => {
        showToast('Protocol number copied!', 'success');
    }).catch(() => {
        // Fallback for older browsers
        const textArea = document.createElement('textarea');
        textArea.value = protocol;
        document.body.appendChild(textArea);
        textArea.select();
        document.execCommand('copy');
        document.body.removeChild(textArea);
        showToast('Protocol number copied!', 'success');
    });
}

// Download PDF receipt
function downloadPDF() {
    const data = window.refundData;
    if (!data) return;

    // Create PDF content (simple text-based for now)
    const content = `
REFUND REQUEST RECEIPT
======================

Protocol: ${data.protocol}
Date: ${new Date().toLocaleDateString()}

CUSTOMER INFORMATION
--------------------
Name: ${data.fullName}
Email: ${data.email}
Phone: ${data.phone}

PURCHASE DETAILS
----------------
Purchase Date: ${data.purchaseDate}
Reason: ${data.reason}

DETAILS
-------
${data.details}

STATUS: Pending Review

---
This is an automated receipt. Please keep this for your records.
Your refund will be processed within 7 business days.

ZapDetect Support Team
    `.trim();

    // Create blob and download
    const blob = new Blob([content], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `refund-receipt-${data.protocol}.txt`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);

    showToast('Receipt downloaded!', 'success');
}

// Show toast notification
function showToast(message, type = 'success') {
    // Remove existing toast
    const existing = document.querySelector('.toast');
    if (existing) {
        existing.remove();
    }

    // Create toast
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.innerHTML = `
        <span>${type === 'success' ? '✓' : '⚠'}</span>
        <span>${message}</span>
    `;

    document.body.appendChild(toast);

    // Remove after 3 seconds
    setTimeout(() => {
        toast.remove();
    }, 3000);
}

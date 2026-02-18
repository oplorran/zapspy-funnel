/**
 * Página de Solicitud de Reembolso - Script
 * Maneja validación de formulario, navegación de pasos y envío
 */

// Datos de países con banderas y códigos de marcación
const countries = [
    { code: 'ES', name: 'España', dial: '+34', flag: '🇪🇸' },
    { code: 'MX', name: 'México', dial: '+52', flag: '🇲🇽' },
    { code: 'AR', name: 'Argentina', dial: '+54', flag: '🇦🇷' },
    { code: 'CO', name: 'Colombia', dial: '+57', flag: '🇨🇴' },
    { code: 'CL', name: 'Chile', dial: '+56', flag: '🇨🇱' },
    { code: 'PE', name: 'Perú', dial: '+51', flag: '🇵🇪' },
    { code: 'VE', name: 'Venezuela', dial: '+58', flag: '🇻🇪' },
    { code: 'EC', name: 'Ecuador', dial: '+593', flag: '🇪🇨' },
    { code: 'UY', name: 'Uruguay', dial: '+598', flag: '🇺🇾' },
    { code: 'PY', name: 'Paraguay', dial: '+595', flag: '🇵🇾' },
    { code: 'BO', name: 'Bolivia', dial: '+591', flag: '🇧🇴' },
    { code: 'GT', name: 'Guatemala', dial: '+502', flag: '🇬🇹' },
    { code: 'CU', name: 'Cuba', dial: '+53', flag: '🇨🇺' },
    { code: 'DO', name: 'República Dominicana', dial: '+1', flag: '🇩🇴' },
    { code: 'HN', name: 'Honduras', dial: '+504', flag: '🇭🇳' },
    { code: 'SV', name: 'El Salvador', dial: '+503', flag: '🇸🇻' },
    { code: 'NI', name: 'Nicaragua', dial: '+505', flag: '🇳🇮' },
    { code: 'CR', name: 'Costa Rica', dial: '+506', flag: '🇨🇷' },
    { code: 'PA', name: 'Panamá', dial: '+507', flag: '🇵🇦' },
    { code: 'PR', name: 'Puerto Rico', dial: '+1', flag: '🇵🇷' },
    { code: 'US', name: 'Estados Unidos', dial: '+1', flag: '🇺🇸' },
    { code: 'BR', name: 'Brasil', dial: '+55', flag: '🇧🇷' },
    { code: 'PT', name: 'Portugal', dial: '+351', flag: '🇵🇹' },
    { code: 'GB', name: 'Reino Unido', dial: '+44', flag: '🇬🇧' },
    { code: 'CA', name: 'Canadá', dial: '+1', flag: '🇨🇦' },
    { code: 'AU', name: 'Australia', dial: '+61', flag: '🇦🇺' },
    { code: 'DE', name: 'Alemania', dial: '+49', flag: '🇩🇪' },
    { code: 'FR', name: 'Francia', dial: '+33', flag: '🇫🇷' },
    { code: 'IT', name: 'Italia', dial: '+39', flag: '🇮🇹' },
    { code: 'JP', name: 'Japón', dial: '+81', flag: '🇯🇵' },
    { code: 'CN', name: 'China', dial: '+86', flag: '🇨🇳' },
    { code: 'IN', name: 'India', dial: '+91', flag: '🇮🇳' },
    { code: 'KR', name: 'Corea del Sur', dial: '+82', flag: '🇰🇷' },
    { code: 'RU', name: 'Rusia', dial: '+7', flag: '🇷🇺' },
    { code: 'ZA', name: 'Sudáfrica', dial: '+27', flag: '🇿🇦' },
    { code: 'NL', name: 'Países Bajos', dial: '+31', flag: '🇳🇱' },
    { code: 'BE', name: 'Bélgica', dial: '+32', flag: '🇧🇪' },
    { code: 'CH', name: 'Suiza', dial: '+41', flag: '🇨🇭' },
    { code: 'AT', name: 'Austria', dial: '+43', flag: '🇦🇹' },
    { code: 'SE', name: 'Suecia', dial: '+46', flag: '🇸🇪' },
    { code: 'NO', name: 'Noruega', dial: '+47', flag: '🇳🇴' },
    { code: 'DK', name: 'Dinamarca', dial: '+45', flag: '🇩🇰' },
    { code: 'FI', name: 'Finlandia', dial: '+358', flag: '🇫🇮' },
    { code: 'PL', name: 'Polonia', dial: '+48', flag: '🇵🇱' },
    { code: 'GR', name: 'Grecia', dial: '+30', flag: '🇬🇷' },
    { code: 'IE', name: 'Irlanda', dial: '+353', flag: '🇮🇪' },
    { code: 'NZ', name: 'Nueva Zelanda', dial: '+64', flag: '🇳🇿' },
    { code: 'SG', name: 'Singapur', dial: '+65', flag: '🇸🇬' },
    { code: 'MY', name: 'Malasia', dial: '+60', flag: '🇲🇾' },
    { code: 'TH', name: 'Tailandia', dial: '+66', flag: '🇹🇭' },
    { code: 'PH', name: 'Filipinas', dial: '+63', flag: '🇵🇭' },
    { code: 'ID', name: 'Indonesia', dial: '+62', flag: '🇮🇩' },
    { code: 'VN', name: 'Vietnam', dial: '+84', flag: '🇻🇳' },
    { code: 'AE', name: 'Emiratos Árabes Unidos', dial: '+971', flag: '🇦🇪' },
    { code: 'SA', name: 'Arabia Saudita', dial: '+966', flag: '🇸🇦' },
    { code: 'IL', name: 'Israel', dial: '+972', flag: '🇮🇱' },
    { code: 'TR', name: 'Turquía', dial: '+90', flag: '🇹🇷' },
    { code: 'EG', name: 'Egipto', dial: '+20', flag: '🇪🇬' },
    { code: 'NG', name: 'Nigeria', dial: '+234', flag: '🇳🇬' },
    { code: 'KE', name: 'Kenia', dial: '+254', flag: '🇰🇪' }
];

let selectedCountry = countries[0];
let currentStep = 1;
let visitorId = null;

// Inicializar página
document.addEventListener('DOMContentLoaded', function() {
    initCountrySelector();
    initCharCounter();
    initFormValidation();
    setMaxDate();
    initFingerprint();
});

// Inicializar FingerprintJS para obtener visitorId
async function initFingerprint() {
    try {
        if (typeof FingerprintJS !== 'undefined') {
            const fp = await FingerprintJS.load();
            const result = await fp.get();
            visitorId = result.visitorId;
            console.log('🔗 Página de reembolso: visitorId capturado:', visitorId);
        } else {
            const script = document.createElement('script');
            script.src = 'https://cdn.jsdelivr.net/npm/@fingerprintjs/fingerprintjs@3/dist/fp.min.js';
            script.onload = async function() {
                const fp = await FingerprintJS.load();
                const result = await fp.get();
                visitorId = result.visitorId;
                console.log('🔗 Página de reembolso: visitorId capturado (dinámico):', visitorId);
            };
            document.head.appendChild(script);
        }
    } catch (error) {
        console.warn('⚠️ Falló la inicialización de FingerprintJS:', error);
    }
}

// Establecer fecha máxima para la fecha de compra (hoy)
function setMaxDate() {
    const today = new Date().toISOString().split('T')[0];
    document.getElementById('purchaseDate').setAttribute('max', today);
}

// Inicializar selector de país
function initCountrySelector() {
    const countryList = document.getElementById('countryList');
    const countrySelector = document.getElementById('countrySelector');
    const countryDropdown = document.getElementById('countryDropdown');
    const countrySearch = document.getElementById('countrySearch');
    const selectedCountryEl = document.getElementById('selectedCountry');

    // Renderizar lista de países
    function renderCountries(filter = '') {
        const filtered = countries.filter(c => 
            c.name.toLowerCase().includes(filter.toLowerCase()) ||
            c.dial.includes(filter)
        );

        if (filtered.length === 0) {
            countryList.innerHTML = '<div class="no-results">No se encontró el país</div>';
            return;
        }

        countryList.innerHTML = filtered.map(country => `
            <div class="country-item ${country.code === selectedCountry.code ? 'selected' : ''}" data-code="${country.code}">
                <span class="flag">${country.flag}</span>
                <span class="name">${country.name}</span>
                <span class="dial-code">${country.dial}</span>
            </div>
        `).join('');

        // Agregar manejadores de clic
        countryList.querySelectorAll('.country-item').forEach(item => {
            item.addEventListener('click', function() {
                const code = this.dataset.code;
                selectCountry(code);
                closeDropdown();
            });
        });
    }

    // Seleccionar país
    function selectCountry(code) {
        selectedCountry = countries.find(c => c.code === code);
        selectedCountryEl.innerHTML = `
            <span class="flag">${selectedCountry.flag}</span>
            <span class="code">${selectedCountry.dial}</span>
            <span class="arrow">▼</span>
        `;
        // Re-renderizar para actualizar estado seleccionado
        renderCountries(countrySearch.value);
    }

    // Abrir dropdown
    function openDropdown() {
        countryDropdown.classList.add('active');
        countrySelector.classList.add('open');
        countrySearch.value = '';
        countrySearch.focus();
        renderCountries();
        
        // Desplazar al país seleccionado
        setTimeout(() => {
            const selectedItem = countryList.querySelector('.country-item.selected');
            if (selectedItem) {
                selectedItem.scrollIntoView({ block: 'center', behavior: 'smooth' });
            }
        }, 100);
    }

    // Cerrar dropdown
    function closeDropdown() {
        countryDropdown.classList.remove('active');
        countrySelector.classList.remove('open');
    }

    // Alternar dropdown
    selectedCountryEl.addEventListener('click', function(e) {
        e.stopPropagation();
        if (countryDropdown.classList.contains('active')) {
            closeDropdown();
        } else {
            openDropdown();
        }
    });

    // Funcionalidad de búsqueda
    countrySearch.addEventListener('input', function() {
        renderCountries(this.value);
    });

    // Navegación por teclado
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

    // Cerrar al hacer clic fuera
    document.addEventListener('click', function(e) {
        if (!countrySelector.contains(e.target)) {
            closeDropdown();
        }
    });

    // Renderizado inicial
    renderCountries();
}

// Inicializar contador de caracteres para detalles (eliminado - ya no es necesario)
function initCharCounter() {
    // Funcionalidad eliminada
}

// Inicializar validación del formulario
function initFormValidation() {
    const form = document.getElementById('refundForm');
    
    form.addEventListener('submit', function(e) {
        e.preventDefault();
        
        if (validateStep(2)) {
            submitForm();
        }
    });
}

// Navegar al siguiente paso
function nextStep(step) {
    if (validateStep(currentStep)) {
        // Marcar paso actual como completado
        document.querySelector(`.progress-step[data-step="${currentStep}"]`).classList.add('completed');
        
        // Actualizar paso actual
        currentStep = step;
        
        // Actualizar progreso
        updateProgress(step);
        
        // Mostrar nuevo paso
        showStep(step);
    }
}

// Navegar al paso anterior
function prevStep(step) {
    currentStep = step;
    updateProgress(step);
    showStep(step);
}

// Actualizar indicador de progreso
function updateProgress(step) {
    document.querySelectorAll('.progress-step').forEach(el => {
        const stepNum = parseInt(el.dataset.step);
        el.classList.remove('active');
        if (stepNum === step) {
            el.classList.add('active');
        }
    });
}

// Mostrar paso específico
function showStep(step) {
    document.querySelectorAll('.form-step').forEach(el => {
        el.classList.remove('active');
    });
    document.querySelector(`.form-step[data-step="${step}"]`).classList.add('active');
    
    // Desplazar hacia arriba
    window.scrollTo({ top: 0, behavior: 'smooth' });
}

// Validar paso actual
function validateStep(step) {
    let isValid = true;

    if (step === 1) {
        // Validar nombre completo
        const fullName = document.getElementById('fullName');
        if (!fullName.value.trim() || fullName.value.trim().length < 3) {
            showError('fullName', 'Por favor, ingresa tu nombre completo');
            isValid = false;
        } else {
            clearError('fullName');
        }

        // Validar correo electrónico
        const email = document.getElementById('email');
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        if (!email.value.trim() || !emailRegex.test(email.value)) {
            showError('email', 'Por favor, ingresa un correo electrónico válido');
            isValid = false;
        } else {
            clearError('email');
        }

        // Validar teléfono
        const phone = document.getElementById('phone');
        if (!phone.value.trim() || phone.value.length < 6) {
            showError('phone', 'Por favor, ingresa un número de teléfono válido');
            isValid = false;
        } else {
            clearError('phone');
        }
    }

    if (step === 2) {
        // Validar fecha de compra
        const purchaseDate = document.getElementById('purchaseDate');
        if (!purchaseDate.value) {
            showError('purchaseDate', 'Por favor, selecciona la fecha de compra');
            isValid = false;
        } else {
            clearError('purchaseDate');
        }

        // Validar motivo
        const reason = document.getElementById('reason');
        if (!reason.value) {
            showError('reason', 'Por favor, selecciona un motivo');
            isValid = false;
        } else {
            clearError('reason');
        }

        // Validar detalles
        const details = document.getElementById('details');
        if (!details.value.trim()) {
            showError('details', 'Por favor, proporciona detalles sobre tu solicitud');
            isValid = false;
        } else {
            clearError('details');
        }
    }

    return isValid;
}

// Mostrar mensaje de error
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

// Limpiar mensaje de error
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

// Enviar formulario
async function submitForm() {
    const submitBtn = document.getElementById('submitBtn');
    submitBtn.classList.add('btn-loading');
    submitBtn.disabled = true;

    // Recopilar datos del formulario
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

    // Generar número de protocolo
    const protocol = generateProtocol();

    try {
        // Enviar al backend
        await sendRefundRequest(formData, protocol);

        // Actualizar resumen
        document.getElementById('protocolNumber').textContent = protocol;
        document.getElementById('summaryName').textContent = formData.fullName;
        document.getElementById('summaryEmail').textContent = formData.email;
        document.getElementById('summaryPhone').textContent = formData.phone;

        // Marcar paso 2 como completado
        document.querySelector('.progress-step[data-step="2"]').classList.add('completed');

        // Ir al paso de confirmación
        currentStep = 3;
        updateProgress(3);
        showStep(3);

        // Almacenar para PDF
        window.refundData = { ...formData, protocol };

    } catch (error) {
        console.error('Error al enviar reembolso:', error);
        showToast('Error al enviar la solicitud. Por favor, inténtalo de nuevo.', 'error');
    } finally {
        submitBtn.classList.remove('btn-loading');
        submitBtn.disabled = false;
    }
}

// Generar número de protocolo
function generateProtocol() {
    const timestamp = Date.now().toString(36).toUpperCase();
    const random = Math.random().toString(36).substr(2, 4).toUpperCase();
    return `REF-${timestamp}${random}`;
}

// Enviar solicitud de reembolso al backend
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
                language: 'es',
                visitorId: visitorId || null
            })
        });

        if (!response.ok) {
            throw new Error('Error al enviar la solicitud de reembolso');
        }

        return await response.json();
    } catch (error) {
        console.warn('Backend no disponible, almacenando localmente:', error);
        // Almacenar localmente como respaldo
        const refunds = JSON.parse(localStorage.getItem('refundRequests') || '[]');
        refunds.push({ ...data, protocol, status: 'pending' });
        localStorage.setItem('refundRequests', JSON.stringify(refunds));
        return { success: true, protocol };
    }
}

// Copiar protocolo al portapapeles
function copyProtocol() {
    const protocol = document.getElementById('protocolNumber').textContent;
    navigator.clipboard.writeText(protocol).then(() => {
        showToast('¡Número de protocolo copiado!', 'success');
    }).catch(() => {
        // Respaldo para navegadores antiguos
        const textArea = document.createElement('textarea');
        textArea.value = protocol;
        document.body.appendChild(textArea);
        textArea.select();
        document.execCommand('copy');
        document.body.removeChild(textArea);
        showToast('¡Número de protocolo copiado!', 'success');
    });
}

// Descargar recibo PDF
function downloadPDF() {
    const data = window.refundData;
    if (!data) return;

    // Crear contenido del PDF (basado en texto simple por ahora)
    const content = `
RECIBO DE SOLICITUD DE REEMBOLSO
================================

Protocolo: ${data.protocol}
Fecha: ${new Date().toLocaleDateString('es-ES')}

INFORMACIÓN DEL CLIENTE
-----------------------
Nombre: ${data.fullName}
Correo: ${data.email}
Teléfono: ${data.phone}

DETALLES DE LA COMPRA
---------------------
Fecha de Compra: ${data.purchaseDate}
Motivo: ${data.reason}

DETALLES
--------
${data.details}

ESTADO: Pendiente de Revisión

---
Este es un recibo automático. Por favor, consérvalo para tus registros.
Tu reembolso será procesado en un plazo de 7 días hábiles.

Equipo de Soporte ZapDetect
    `.trim();

    // Crear blob y descargar
    const blob = new Blob([content], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `recibo-reembolso-${data.protocol}.txt`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);

    showToast('¡Recibo descargado!', 'success');
}

// Mostrar notificación toast
function showToast(message, type = 'success') {
    // Eliminar toast existente
    const existing = document.querySelector('.toast');
    if (existing) {
        existing.remove();
    }

    // Crear toast
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.innerHTML = `
        <span>${type === 'success' ? '✓' : '⚠'}</span>
        <span>${message}</span>
    `;

    document.body.appendChild(toast);

    // Eliminar después de 3 segundos
    setTimeout(() => {
        toast.remove();
    }, 3000);
}

/**
 * ZapSpy.ai - Email/WhatsApp Capture Modal (Spanish Version)
 * Captura contacto del usuario antes de redirigir al checkout
 */

const ZAPSPY_API_URL = 'https://zapspy-funnel-production.up.railway.app';

const EmailCapture = {
    modalShown: false,
    
    // Lista completa de códigos de país (países hispanohablantes primero)
    countryCodes: `
        <!-- Países hispanohablantes primero -->
        <option value="+52" selected>🇲🇽 +52</option>
        <option value="+34">🇪🇸 +34</option>
        <option value="+54">🇦🇷 +54</option>
        <option value="+57">🇨🇴 +57</option>
        <option value="+51">🇵🇪 +51</option>
        <option value="+58">🇻🇪 +58</option>
        <option value="+56">🇨🇱 +56</option>
        <option value="+593">🇪🇨 +593</option>
        <option value="+502">🇬🇹 +502</option>
        <option value="+53">🇨🇺 +53</option>
        <option value="+591">🇧🇴 +591</option>
        <option value="+1809">🇩🇴 +1809</option>
        <option value="+504">🇭🇳 +504</option>
        <option value="+595">🇵🇾 +595</option>
        <option value="+503">🇸🇻 +503</option>
        <option value="+505">🇳🇮 +505</option>
        <option value="+506">🇨🇷 +506</option>
        <option value="+507">🇵🇦 +507</option>
        <option value="+598">🇺🇾 +598</option>
        <option value="+1787">🇵🇷 +1787</option>
        <!-- Estados Unidos y Canadá -->
        <option value="+1">🇺🇸 +1</option>
        <option value="+1">🇨🇦 +1</option>
        <!-- Europa -->
        <option value="+44">🇬🇧 +44</option>
        <option value="+49">🇩🇪 +49</option>
        <option value="+33">🇫🇷 +33</option>
        <option value="+39">🇮🇹 +39</option>
        <option value="+351">🇵🇹 +351</option>
        <option value="+31">🇳🇱 +31</option>
        <option value="+32">🇧🇪 +32</option>
        <option value="+41">🇨🇭 +41</option>
        <option value="+43">🇦🇹 +43</option>
        <option value="+46">🇸🇪 +46</option>
        <option value="+47">🇳🇴 +47</option>
        <option value="+45">🇩🇰 +45</option>
        <option value="+358">🇫🇮 +358</option>
        <option value="+48">🇵🇱 +48</option>
        <option value="+30">🇬🇷 +30</option>
        <option value="+7">🇷🇺 +7</option>
        <option value="+90">🇹🇷 +90</option>
        <option value="+380">🇺🇦 +380</option>
        <!-- Brasil -->
        <option value="+55">🇧🇷 +55</option>
        <!-- Asia -->
        <option value="+81">🇯🇵 +81</option>
        <option value="+82">🇰🇷 +82</option>
        <option value="+86">🇨🇳 +86</option>
        <option value="+91">🇮🇳 +91</option>
        <option value="+62">🇮🇩 +62</option>
        <option value="+66">🇹🇭 +66</option>
        <option value="+84">🇻🇳 +84</option>
        <option value="+60">🇲🇾 +60</option>
        <option value="+65">🇸🇬 +65</option>
        <option value="+63">🇵🇭 +63</option>
        <!-- Oriente Medio -->
        <option value="+971">🇦🇪 +971</option>
        <option value="+966">🇸🇦 +966</option>
        <option value="+972">🇮🇱 +972</option>
        <!-- África -->
        <option value="+20">🇪🇬 +20</option>
        <option value="+212">🇲🇦 +212</option>
        <option value="+27">🇿🇦 +27</option>
        <option value="+234">🇳🇬 +234</option>
        <!-- Oceanía -->
        <option value="+61">🇦🇺 +61</option>
        <option value="+64">🇳🇿 +64</option>
    `,
    
    show: function(onSuccess) {
        if (this.modalShown) return;
        this.modalShown = true;
        
        const modal = document.createElement('div');
        modal.id = 'emailCaptureModal';
        modal.setAttribute('role', 'dialog');
        modal.setAttribute('aria-modal', 'true');
        modal.setAttribute('aria-labelledby', 'captureModalTitle');
        modal.innerHTML = `
            <div style="position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.95); z-index: 99999; display: flex; align-items: center; justify-content: center; padding: 20px; backdrop-filter: blur(10px);">
                <div style="background: linear-gradient(135deg, #1F2C33, #111B21); border-radius: 20px; padding: 28px 24px; max-width: 380px; width: 100%; text-align: center; border: 2px solid rgba(37, 211, 102, 0.4); box-shadow: 0 20px 60px rgba(0,0,0,0.5);">
                    <div style="width: 60px; height: 60px; background: linear-gradient(135deg, #25D366, #128C7E); border-radius: 50%; display: flex; align-items: center; justify-content: center; margin: 0 auto 20px;">
                        <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2">
                            <path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07 19.5 19.5 0 0 1-6-6 19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 4.11 2h3a2 2 0 0 1 2 1.72"/>
                        </svg>
                    </div>
                    
                    <h3 id="captureModalTitle" style="color: #E9EDEF; font-size: 22px; font-weight: 700; margin-bottom: 12px;">¿Dónde enviamos tu informe?</h3>
                    <p style="color: #8696A0; font-size: 14px; margin-bottom: 24px; line-height: 1.5;">
                        Después de la compra, enviaremos el <strong style="color: #25D366;">informe completo de espionaje</strong> con todos los mensajes recuperados, fotos y ubicaciones a tu email y WhatsApp.
                    </p>
                    
                    <form id="captureForm" style="display: flex; flex-direction: column; gap: 12px; margin-bottom: 16px;">
                        <div style="position: relative;">
                            <input 
                                type="text" 
                                id="captureName" 
                                placeholder="Tu nombre"
                                required
                                aria-label="Tu nombre"
                                style="width: 100%; padding: 16px 16px 16px 44px; background: #111B21; border: 2px solid #2a3942; border-radius: 12px; color: #E9EDEF; font-size: 16px; outline: none; transition: border-color 0.3s; box-sizing: border-box;"
                            >
                            <svg style="position: absolute; left: 14px; top: 50%; transform: translateY(-50%); width: 20px; height: 20px; color: #667781;" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/>
                                <circle cx="12" cy="7" r="4"/>
                            </svg>
                        </div>
                        
                        <div style="position: relative;">
                            <input 
                                type="email" 
                                id="captureEmail" 
                                placeholder="Tu mejor email"
                                required
                                aria-label="Tu correo electrónico"
                                style="width: 100%; padding: 16px 16px 16px 44px; background: #111B21; border: 2px solid #2a3942; border-radius: 12px; color: #E9EDEF; font-size: 16px; outline: none; transition: border-color 0.3s; box-sizing: border-box;"
                            >
                            <svg style="position: absolute; left: 14px; top: 50%; transform: translateY(-50%); width: 20px; height: 20px; color: #667781;" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/>
                                <polyline points="22,6 12,13 2,6"/>
                            </svg>
                        </div>
                        
                        <div style="display: flex; gap: 8px;">
                            <select 
                                id="captureCountry" 
                                aria-label="Código de país"
                                style="width: 110px; padding: 16px 8px; background: #111B21; border: 2px solid #2a3942; border-radius: 12px; color: #E9EDEF; font-size: 14px; outline: none; cursor: pointer;"
                            >
                                ${this.countryCodes}
                            </select>
                            <div style="position: relative; flex: 1;">
                                <input 
                                    type="tel" 
                                    id="captureWhatsApp" 
                                    placeholder="Tu número de WhatsApp"
                                    required
                                    aria-label="Tu número de WhatsApp"
                                    style="width: 100%; padding: 16px 16px 16px 44px; background: #111B21; border: 2px solid #2a3942; border-radius: 12px; color: #E9EDEF; font-size: 16px; outline: none; transition: border-color 0.3s; box-sizing: border-box;"
                                >
                                <svg style="position: absolute; left: 14px; top: 50%; transform: translateY(-50%); width: 20px; height: 20px; color: #25D366;" viewBox="0 0 24 24" fill="currentColor">
                                    <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.57-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347z"/>
                                </svg>
                            </div>
                        </div>
                        
                        <button 
                            type="submit" 
                            style="width: 100%; padding: 18px; background: linear-gradient(135deg, #25D366, #128C7E); border: none; border-radius: 12px; color: white; font-size: 17px; font-weight: 700; cursor: pointer; transition: transform 0.2s, box-shadow 0.2s; box-shadow: 0 4px 20px rgba(37, 211, 102, 0.3);"
                        >
                            Continuar al Pago Seguro →
                        </button>
                    </form>
                    
                    <p style="font-size: 11px; color: #667781; display: flex; align-items: center; justify-content: center; gap: 6px;">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#25D366" stroke-width="2">
                            <rect x="3" y="11" width="18" height="11" rx="2" ry="2"></rect>
                            <path d="M7 11V7a5 5 0 0 1 10 0v4"></path>
                        </svg>
                        Tus datos están encriptados y seguros
                    </p>
                    
                    <button 
                        id="skipCaptureBtn"
                        style="margin-top: 12px; padding: 10px; background: transparent; border: none; color: #667781; font-size: 13px; cursor: pointer; text-decoration: underline;"
                    >
                        Saltar y continuar al pago
                    </button>
                </div>
            </div>
        `;
        
        document.body.appendChild(modal);
        
        setTimeout(() => {
            document.getElementById('captureName').focus();
        }, 100);
        
        document.getElementById('captureForm').addEventListener('submit', (e) => {
            e.preventDefault();
            
            const name = document.getElementById('captureName').value;
            const email = document.getElementById('captureEmail').value;
            const country = document.getElementById('captureCountry').value;
            const whatsapp = document.getElementById('captureWhatsApp').value;
            
            localStorage.setItem('userName', name);
            localStorage.setItem('userEmail', email);
            localStorage.setItem('userCountryCode', country);
            localStorage.setItem('userPhoneNumber', whatsapp);
            localStorage.setItem('userWhatsApp', country + whatsapp);
            
            this.sendToBackend({
                name: name,
                email: email,
                whatsapp: country + whatsapp,
                targetPhone: localStorage.getItem('targetPhone') || '',
                targetGender: localStorage.getItem('targetGender') || '',
                funnelLanguage: 'es',
                timestamp: new Date().toISOString(),
                userAgent: navigator.userAgent,
                referrer: document.referrer,
                fbc: localStorage.getItem('_fbc') || '',
                fbp: localStorage.getItem('_fbp') || ''
            });
            
            if (typeof FunnelTracker !== 'undefined') {
                FunnelTracker.track(FunnelTracker.events.EMAIL_CAPTURED);
            }
            
            modal.remove();
            this.modalShown = false;
            
            if (typeof onSuccess === 'function') {
                onSuccess({ name, email, whatsapp: country + whatsapp });
            }
        });
        
        document.getElementById('skipCaptureBtn').addEventListener('click', () => {
            modal.remove();
            this.modalShown = false;
            
            if (typeof onSuccess === 'function') {
                onSuccess(null);
            }
        });
        
        ['captureName', 'captureEmail', 'captureWhatsApp'].forEach(id => {
            const input = document.getElementById(id);
            if (input) {
                input.addEventListener('focus', () => {
                    input.style.borderColor = '#25D366';
                });
                input.addEventListener('blur', () => {
                    input.style.borderColor = '#2a3942';
                });
            }
        });
    },
    
    sendToBackend: function(data) {
        if (!ZAPSPY_API_URL) {
            console.log('Lead captured (backend not configured):', data);
            return;
        }
        
        fetch(`${ZAPSPY_API_URL}/api/leads`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(data)
        })
        .then(response => response.json())
        .then(result => {
            if (result.success) {
                console.log('Lead saved to database successfully');
            } else {
                console.error('Error saving lead:', result.error);
            }
        })
        .catch(error => {
            console.error('Error sending lead to backend:', error);
        });
    }
};

window.EmailCapture = EmailCapture;

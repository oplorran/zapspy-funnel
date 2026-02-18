/**
 * ZapSpy.ai - Email/WhatsApp Capture Modal
 * Captures user contact before redirecting to checkout
 */

// ============================================================
// CONFIGURE YOUR BACKEND URL HERE
// After deploying to Railway, paste your URL below:
// Example: 'https://zapspy-backend.up.railway.app'
// ============================================================
const ZAPSPY_API_URL = 'https://zapspy-funnel-production.up.railway.app'; // Railway backend URL

const EmailCapture = {
    modalShown: false,
    
    // Complete list of country codes (same as phone.html)
    countryCodes: `
        <!-- English-speaking countries first -->
        <option value="+1" selected>🇺🇸 +1</option>
        <option value="+1">🇨🇦 +1</option>
        <option value="+44">🇬🇧 +44</option>
        <option value="+61">🇦🇺 +61</option>
        <option value="+64">🇳🇿 +64</option>
        <option value="+353">🇮🇪 +353</option>
        <option value="+27">🇿🇦 +27</option>
        <option value="+65">🇸🇬 +65</option>
        <option value="+63">🇵🇭 +63</option>
        <option value="+91">🇮🇳 +91</option>
        <option value="+234">🇳🇬 +234</option>
        <option value="+254">🇰🇪 +254</option>
        <option value="+233">🇬🇭 +233</option>
        <option value="+256">🇺🇬 +256</option>
        <option value="+255">🇹🇿 +255</option>
        <option value="+263">🇿🇼 +263</option>
        <option value="+260">🇿🇲 +260</option>
        <option value="+267">🇧🇼 +267</option>
        <option value="+1876">🇯🇲 +1876</option>
        <option value="+1868">🇹🇹 +1868</option>
        <option value="+1246">🇧🇧 +1246</option>
        <option value="+1242">🇧🇸 +1242</option>
        <!-- European countries -->
        <option value="+49">🇩🇪 +49</option>
        <option value="+33">🇫🇷 +33</option>
        <option value="+34">🇪🇸 +34</option>
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
        <option value="+420">🇨🇿 +420</option>
        <option value="+36">🇭🇺 +36</option>
        <option value="+40">🇷🇴 +40</option>
        <option value="+359">🇧🇬 +359</option>
        <option value="+381">🇷🇸 +381</option>
        <option value="+385">🇭🇷 +385</option>
        <option value="+386">🇸🇮 +386</option>
        <option value="+421">🇸🇰 +421</option>
        <option value="+370">🇱🇹 +370</option>
        <option value="+371">🇱🇻 +371</option>
        <option value="+372">🇪🇪 +372</option>
        <option value="+375">🇧🇾 +375</option>
        <option value="+373">🇲🇩 +373</option>
        <option value="+355">🇦🇱 +355</option>
        <option value="+389">🇲🇰 +389</option>
        <option value="+387">🇧🇦 +387</option>
        <option value="+382">🇲🇪 +382</option>
        <option value="+383">🇽🇰 +383</option>
        <option value="+354">🇮🇸 +354</option>
        <option value="+352">🇱🇺 +352</option>
        <option value="+356">🇲🇹 +356</option>
        <option value="+357">🇨🇾 +357</option>
        <option value="+376">🇦🇩 +376</option>
        <option value="+377">🇲🇨 +377</option>
        <option value="+378">🇸🇲 +378</option>
        <option value="+423">🇱🇮 +423</option>
        <!-- Latin America -->
        <option value="+55">🇧🇷 +55</option>
        <option value="+52">🇲🇽 +52</option>
        <option value="+54">🇦🇷 +54</option>
        <option value="+57">🇨🇴 +57</option>
        <option value="+56">🇨🇱 +56</option>
        <option value="+51">🇵🇪 +51</option>
        <option value="+58">🇻🇪 +58</option>
        <option value="+593">🇪🇨 +593</option>
        <option value="+591">🇧🇴 +591</option>
        <option value="+595">🇵🇾 +595</option>
        <option value="+598">🇺🇾 +598</option>
        <option value="+502">🇬🇹 +502</option>
        <option value="+503">🇸🇻 +503</option>
        <option value="+504">🇭🇳 +504</option>
        <option value="+505">🇳🇮 +505</option>
        <option value="+506">🇨🇷 +506</option>
        <option value="+507">🇵🇦 +507</option>
        <option value="+53">🇨🇺 +53</option>
        <option value="+1809">🇩🇴 +1809</option>
        <option value="+1787">🇵🇷 +1787</option>
        <option value="+509">🇭🇹 +509</option>
        <option value="+501">🇧🇿 +501</option>
        <option value="+592">🇬🇾 +592</option>
        <option value="+597">🇸🇷 +597</option>
        <option value="+594">🇬🇫 +594</option>
        <!-- Caribbean -->
        <option value="+1264">🇦🇮 +1264</option>
        <option value="+1268">🇦🇬 +1268</option>
        <option value="+297">🇦🇼 +297</option>
        <option value="+1441">🇧🇲 +1441</option>
        <option value="+599">🇧🇶 +599</option>
        <option value="+1284">🇻🇬 +1284</option>
        <option value="+1345">🇰🇾 +1345</option>
        <option value="+5999">🇨🇼 +5999</option>
        <option value="+1767">🇩🇲 +1767</option>
        <option value="+1473">🇬🇩 +1473</option>
        <option value="+590">🇬🇵 +590</option>
        <option value="+596">🇲🇶 +596</option>
        <option value="+1664">🇲🇸 +1664</option>
        <option value="+1869">🇰🇳 +1869</option>
        <option value="+1758">🇱🇨 +1758</option>
        <option value="+1721">🇸🇽 +1721</option>
        <option value="+1784">🇻🇨 +1784</option>
        <option value="+1649">🇹🇨 +1649</option>
        <option value="+1340">🇻🇮 +1340</option>
        <!-- Asia -->
        <option value="+81">🇯🇵 +81</option>
        <option value="+82">🇰🇷 +82</option>
        <option value="+86">🇨🇳 +86</option>
        <option value="+852">🇭🇰 +852</option>
        <option value="+853">🇲🇴 +853</option>
        <option value="+886">🇹🇼 +886</option>
        <option value="+62">🇮🇩 +62</option>
        <option value="+66">🇹🇭 +66</option>
        <option value="+84">🇻🇳 +84</option>
        <option value="+60">🇲🇾 +60</option>
        <option value="+855">🇰🇭 +855</option>
        <option value="+856">🇱🇦 +856</option>
        <option value="+95">🇲🇲 +95</option>
        <option value="+880">🇧🇩 +880</option>
        <option value="+92">🇵🇰 +92</option>
        <option value="+94">🇱🇰 +94</option>
        <option value="+977">🇳🇵 +977</option>
        <option value="+975">🇧🇹 +975</option>
        <option value="+960">🇲🇻 +960</option>
        <option value="+93">🇦🇫 +93</option>
        <option value="+850">🇰🇵 +850</option>
        <option value="+976">🇲🇳 +976</option>
        <option value="+673">🇧🇳 +673</option>
        <option value="+670">🇹🇱 +670</option>
        <!-- Middle East -->
        <option value="+971">🇦🇪 +971</option>
        <option value="+966">🇸🇦 +966</option>
        <option value="+972">🇮🇱 +972</option>
        <option value="+962">🇯🇴 +962</option>
        <option value="+961">🇱🇧 +961</option>
        <option value="+963">🇸🇾 +963</option>
        <option value="+964">🇮🇶 +964</option>
        <option value="+98">🇮🇷 +98</option>
        <option value="+965">🇰🇼 +965</option>
        <option value="+974">🇶🇦 +974</option>
        <option value="+973">🇧🇭 +973</option>
        <option value="+968">🇴🇲 +968</option>
        <option value="+967">🇾🇪 +967</option>
        <option value="+970">🇵🇸 +970</option>
        <!-- Central Asia -->
        <option value="+7">🇰🇿 +7</option>
        <option value="+998">🇺🇿 +998</option>
        <option value="+993">🇹🇲 +993</option>
        <option value="+992">🇹🇯 +992</option>
        <option value="+996">🇰🇬 +996</option>
        <option value="+994">🇦🇿 +994</option>
        <option value="+995">🇬🇪 +995</option>
        <option value="+374">🇦🇲 +374</option>
        <!-- Africa -->
        <option value="+20">🇪🇬 +20</option>
        <option value="+212">🇲🇦 +212</option>
        <option value="+213">🇩🇿 +213</option>
        <option value="+216">🇹🇳 +216</option>
        <option value="+218">🇱🇾 +218</option>
        <option value="+249">🇸🇩 +249</option>
        <option value="+251">🇪🇹 +251</option>
        <option value="+252">🇸🇴 +252</option>
        <option value="+253">🇩🇯 +253</option>
        <option value="+291">🇪🇷 +291</option>
        <option value="+221">🇸🇳 +221</option>
        <option value="+220">🇬🇲 +220</option>
        <option value="+224">🇬🇳 +224</option>
        <option value="+225">🇨🇮 +225</option>
        <option value="+226">🇧🇫 +226</option>
        <option value="+227">🇳🇪 +227</option>
        <option value="+228">🇹🇬 +228</option>
        <option value="+229">🇧🇯 +229</option>
        <option value="+230">🇲🇺 +230</option>
        <option value="+231">🇱🇷 +231</option>
        <option value="+232">🇸🇱 +232</option>
        <option value="+235">🇹🇩 +235</option>
        <option value="+236">🇨🇫 +236</option>
        <option value="+237">🇨🇲 +237</option>
        <option value="+238">🇨🇻 +238</option>
        <option value="+239">🇸🇹 +239</option>
        <option value="+240">🇬🇶 +240</option>
        <option value="+241">🇬🇦 +241</option>
        <option value="+242">🇨🇬 +242</option>
        <option value="+243">🇨🇩 +243</option>
        <option value="+244">🇦🇴 +244</option>
        <option value="+245">🇬🇼 +245</option>
        <option value="+248">🇸🇨 +248</option>
        <option value="+250">🇷🇼 +250</option>
        <option value="+257">🇧🇮 +257</option>
        <option value="+258">🇲🇿 +258</option>
        <option value="+261">🇲🇬 +261</option>
        <option value="+262">🇷🇪 +262</option>
        <option value="+264">🇳🇦 +264</option>
        <option value="+265">🇲🇼 +265</option>
        <option value="+266">🇱🇸 +266</option>
        <option value="+268">🇸🇿 +268</option>
        <option value="+269">🇰🇲 +269</option>
        <option value="+290">🇸🇭 +290</option>
        <!-- Oceania -->
        <option value="+675">🇵🇬 +675</option>
        <option value="+679">🇫🇯 +679</option>
        <option value="+676">🇹🇴 +676</option>
        <option value="+677">🇸🇧 +677</option>
        <option value="+678">🇻🇺 +678</option>
        <option value="+680">🇵🇼 +680</option>
        <option value="+681">🇼🇫 +681</option>
        <option value="+682">🇨🇰 +682</option>
        <option value="+683">🇳🇺 +683</option>
        <option value="+685">🇼🇸 +685</option>
        <option value="+686">🇰🇮 +686</option>
        <option value="+687">🇳🇨 +687</option>
        <option value="+688">🇹🇻 +688</option>
        <option value="+689">🇵🇫 +689</option>
        <option value="+690">🇹🇰 +690</option>
        <option value="+691">🇫🇲 +691</option>
        <option value="+692">🇲🇭 +692</option>
        <option value="+674">🇳🇷 +674</option>
    `,
    
    /**
     * Show the capture modal
     * @param {Function} onSuccess - Callback when form is submitted
     */
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
                    
                    <h3 id="captureModalTitle" style="color: #E9EDEF; font-size: 22px; font-weight: 700; margin-bottom: 12px;">Where should we send your report?</h3>
                    <p style="color: #8696A0; font-size: 14px; margin-bottom: 24px; line-height: 1.5;">
                        After purchase, we'll send the <strong style="color: #25D366;">complete spy report</strong> with all recovered messages, photos and locations to your email and WhatsApp.
                    </p>
                    
                    <form id="captureForm" style="display: flex; flex-direction: column; gap: 12px; margin-bottom: 16px;">
                        <div style="position: relative;">
                            <input 
                                type="email" 
                                id="captureEmail" 
                                placeholder="Your best email"
                                required
                                aria-label="Your email address"
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
                                aria-label="Country code"
                                style="width: 110px; padding: 16px 8px; background: #111B21; border: 2px solid #2a3942; border-radius: 12px; color: #E9EDEF; font-size: 14px; outline: none; cursor: pointer;"
                            >
                                ${this.countryCodes}
                            </select>
                            <div style="position: relative; flex: 1;">
                                <input 
                                    type="tel" 
                                    id="captureWhatsApp" 
                                    placeholder="Your WhatsApp number"
                                    required
                                    aria-label="Your WhatsApp number"
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
                            Continue to Secure Payment →
                        </button>
                    </form>
                    
                    <p style="font-size: 11px; color: #667781; display: flex; align-items: center; justify-content: center; gap: 6px;">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#25D366" stroke-width="2">
                            <rect x="3" y="11" width="18" height="11" rx="2" ry="2"></rect>
                            <path d="M7 11V7a5 5 0 0 1 10 0v4"></path>
                        </svg>
                        Your data is encrypted and secure
                    </p>
                    
                    <button 
                        id="skipCaptureBtn"
                        style="margin-top: 12px; padding: 10px; background: transparent; border: none; color: #667781; font-size: 13px; cursor: pointer; text-decoration: underline;"
                    >
                        Skip and continue to payment
                    </button>
                </div>
            </div>
        `;
        
        document.body.appendChild(modal);
        
        // Focus first input for accessibility
        setTimeout(() => {
            document.getElementById('captureEmail').focus();
        }, 100);
        
        // Form submission handler
        document.getElementById('captureForm').addEventListener('submit', (e) => {
            e.preventDefault();
            
            const email = document.getElementById('captureEmail').value;
            const country = document.getElementById('captureCountry').value;
            const whatsapp = document.getElementById('captureWhatsApp').value;
            
            // Store in localStorage
            localStorage.setItem('userEmail', email);
            localStorage.setItem('userWhatsApp', country + whatsapp);
            
            // Ensure visitorId exists (create if missing)
            let visitorId = localStorage.getItem('funnelVisitorId');
            if (!visitorId) {
                visitorId = 'v_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
                localStorage.setItem('funnelVisitorId', visitorId);
                console.log('📊 Created visitorId in email-capture:', visitorId);
            }
            
            // Send to webhook/backend (if configured)
            this.sendToBackend({
                email: email,
                whatsapp: country + whatsapp,
                targetPhone: localStorage.getItem('targetPhone') || '',
                targetGender: localStorage.getItem('targetGender') || '',
                timestamp: new Date().toISOString(),
                userAgent: navigator.userAgent,
                referrer: document.referrer,
                visitorId: visitorId,
                funnelLanguage: 'en',
                funnelSource: 'perfectpay',
                fbc: localStorage.getItem('_fbc') || '',
                fbp: localStorage.getItem('_fbp') || '',
                // UTM parameters for attribution
                utm_source: localStorage.getItem('utm_source') || '',
                utm_medium: localStorage.getItem('utm_medium') || '',
                utm_campaign: localStorage.getItem('utm_campaign') || '',
                utm_content: localStorage.getItem('utm_content') || '',
                utm_term: localStorage.getItem('utm_term') || '',
                // A/B test tracking
                ab_test_id: localStorage.getItem('ab_test_id') ? parseInt(localStorage.getItem('ab_test_id')) : null,
                ab_variant: localStorage.getItem('ab_variant') || null
            });
            
            // Track CAPI Lead event
            if (typeof FacebookCAPI !== 'undefined') {
                FacebookCAPI.trackLead(email, { phone: country + whatsapp, name: name });
            }
            
            // Track event
            if (typeof ZapSpyTracking !== 'undefined') {
                ZapSpyTracking.trackEvent('LeadCapture', {
                    has_email: true,
                    has_whatsapp: true
                });
            }
            
            // Track in funnel
            if (typeof FunnelTracker !== 'undefined') {
                FunnelTracker.track(FunnelTracker.events.EMAIL_CAPTURED);
            }
            
            // Close modal
            modal.remove();
            this.modalShown = false;
            
            // Execute callback
            if (typeof onSuccess === 'function') {
                onSuccess({ email, whatsapp: country + whatsapp });
            }
        });
        
        // Skip button handler
        document.getElementById('skipCaptureBtn').addEventListener('click', () => {
            modal.remove();
            this.modalShown = false;
            
            // Track event
            if (typeof ZapSpyTracking !== 'undefined') {
                ZapSpyTracking.trackEvent('LeadCapture', {
                    skipped: true
                });
            }
            
            // Execute callback
            if (typeof onSuccess === 'function') {
                onSuccess(null);
            }
        });
        
        // Focus styles for inputs
        ['captureEmail', 'captureWhatsApp'].forEach(id => {
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
    
    /**
     * Send captured data to backend API
     * @param {Object} data - Lead data to send
     */
    sendToBackend: function(data) {
        if (!ZAPSPY_API_URL) {
            console.log('Lead captured (backend not configured):', data);
            console.log('To save leads to database, configure ZAPSPY_API_URL in email-capture.js');
            return;
        }
        
        // Send data to backend API
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

// Export for global use
window.EmailCapture = EmailCapture;

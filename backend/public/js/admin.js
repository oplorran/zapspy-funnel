        const API_URL = window.location.origin;
        let authToken = localStorage.getItem('adminToken');
        let token = authToken; // Sync token with authToken for backward compatibility
        let currentPage = 1;
        let searchTimeout;
        let leadsChart, statusChart;
        let selectedTags = [];
        
        const statusLabels = { 'new': 'Novo', 'contacted': 'Contatado', 'converted': 'Convertido', 'lost': 'Perdido' };
        
        // Convert country code to flag emoji
        function getCountryFlag(countryCode) {
            if (!countryCode) return '🌍';
            const code = countryCode.toUpperCase();
            // Convert country code to regional indicator symbols
            const flag = code.split('').map(char => 
                String.fromCodePoint(0x1F1E6 + char.charCodeAt(0) - 65)
            ).join('');
            return flag;
        }
        
        const pageTitles = {
            'overview': { title: 'Visão Geral', subtitle: 'Acompanhe suas métricas em tempo real' },
            'leads': { title: 'Leads', subtitle: 'Gerencie todos os seus leads' },
            'recovery': { title: 'Recuperação', subtitle: 'Recupere vendas perdidas' },
            'funnel': { title: 'Funil', subtitle: 'Visualize a jornada dos visitantes' },
            'sales': { title: 'Vendas', subtitle: 'Acompanhe suas transações' },
            'refunds': { title: 'Reembolsos', subtitle: 'Gerencie solicitações de reembolso' },
            'countries': { title: 'Países', subtitle: 'Análise de performance por país' },
            'reports': { title: 'Exportar Dados', subtitle: 'Baixe relatórios em Excel, PDF ou CSV' },
            'settings': { title: 'Configurações', subtitle: 'Configure integrações' },
            'users': { title: 'Usuários', subtitle: 'Gerencie membros da equipe' }
        };
        
        const templates = {
            1: `Olá! 👋

Notamos que você não finalizou sua compra no ZapSpy.ai.

⚠️ As vagas são limitadas e estamos quase esgotados!

Garanta seu acesso agora: https://zapspy.ai

Posso ajudar com alguma dúvida?`,
            2: `Ei! 🎁

Temos uma oferta especial de 20% OFF só para você finalizar sua compra no ZapSpy.ai!

Use o cupom: VOLTA20

Aproveite, é por tempo limitado! ⏰`,
            3: `Olá! 👋

Vi que você teve interesse no ZapSpy.ai.

Posso ajudar com alguma dúvida sobre o produto?

Estou aqui para esclarecer tudo! 😊`
        };
        
        // Check if logged in - validate token first
        if (authToken) {
            validateAndShowDashboard();
        }
        
        // Validate token before showing dashboard
        async function validateAndShowDashboard() {
            try {
                // Quick check if token is still valid
                const response = await fetch(`${API_URL}/api/admin/stats`, {
                    headers: { 'Authorization': `Bearer ${authToken}` }
                });
                
                if (response.status === 401 || response.status === 403) {
                    // Token expired or invalid - force re-login
                    console.log('Token expired, forcing re-login');
                    localStorage.removeItem('adminToken');
                    localStorage.removeItem('adminUser');
                    authToken = null;
                    token = null;
                    location.reload();
                    return;
                }
                
                // Token is valid - fetch profile if needed
                const storedUser = localStorage.getItem('adminUser');
                if (!storedUser) {
                    await fetchUserProfile();
                }
                
                showDashboard();
            } catch (error) {
                console.error('Error validating token:', error);
                // On network error, still try to show dashboard
                showDashboard();
            }
        }
        
        // Fetch user profile
        async function fetchUserProfile() {
            try {
                const response = await fetch(`${API_URL}/api/admin/profile`, {
                    headers: { 'Authorization': `Bearer ${authToken}` }
                });
                
                if (response.ok) {
                    const data = await response.json();
                    if (data.user) {
                        localStorage.setItem('adminUser', JSON.stringify(data.user));
                    }
                } else if (response.status === 401 || response.status === 403) {
                    logout();
                }
            } catch (error) {
                console.error('Error fetching profile:', error);
            }
        }
        
        // Login form
        document.getElementById('loginForm').addEventListener('submit', async (e) => {
            e.preventDefault();
            const email = document.getElementById('loginEmail').value;
            const password = document.getElementById('loginPassword').value;
            const errorEl = document.getElementById('loginError');
            
            try {
                const response = await fetch(`${API_URL}/api/admin/login`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ email, password })
                });
                const data = await response.json();
                if (response.ok && data.token) {
                    authToken = data.token;
                    token = data.token; // Update both variables
                    localStorage.setItem('adminToken', authToken);
                    // Store user info for role-based access
                    if (data.user) {
                        localStorage.setItem('adminUser', JSON.stringify(data.user));
                    }
                    showDashboard();
                } else {
                    errorEl.textContent = data.error || 'Credenciais inválidas';
                    errorEl.classList.add('visible');
                }
            } catch (error) {
                errorEl.textContent = 'Erro de conexão';
                errorEl.classList.add('visible');
            }
        });
        
        function showDashboard() {
            document.getElementById('loginScreen').style.display = 'none';
            document.getElementById('dashboard').classList.add('active');
            
            // Get user info and update UI based on role
            const user = getCurrentUser();
            updateUIForRole(user);
            
            loadAllData();
            initCharts();
        }
        
        // Get date range from global filter
        function getGlobalDateRange() {
            const filterElement = document.getElementById('globalDateFilter');
            if (!filterElement) {
                console.warn('⚠️ globalDateFilter element not found');
                return null;
            }
            
            const filter = filterElement.value || 'all';
            console.log('📅 Date filter value:', filter);
            
            if (filter === 'all') return null;
            
            const now = new Date();
            const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
            let startDate, endDate = new Date();
            
            switch(filter) {
                case 'today':
                    startDate = today;
                    break;
                case 'yesterday':
                    startDate = new Date(today);
                    startDate.setDate(today.getDate() - 1);
                    endDate = today;
                    break;
                case 'this_week':
                    startDate = new Date(today);
                    startDate.setDate(today.getDate() - today.getDay()); // Sunday
                    break;
                case 'last_week':
                    startDate = new Date(today);
                    startDate.setDate(today.getDate() - today.getDay() - 7);
                    endDate = new Date(today);
                    endDate.setDate(today.getDate() - today.getDay());
                    break;
                case 'this_month':
                    startDate = new Date(now.getFullYear(), now.getMonth(), 1);
                    break;
                case 'last_month':
                    startDate = new Date(now.getFullYear(), now.getMonth() - 1, 1);
                    endDate = new Date(now.getFullYear(), now.getMonth(), 0);
                    break;
                case 'last_7':
                    startDate = new Date(today);
                    startDate.setDate(today.getDate() - 7);
                    break;
                case 'last_15':
                    startDate = new Date(today);
                    startDate.setDate(today.getDate() - 15);
                    break;
                case 'last_30':
                    startDate = new Date(today);
                    startDate.setDate(today.getDate() - 30);
                    break;
                case 'last_60':
                    startDate = new Date(today);
                    startDate.setDate(today.getDate() - 60);
                    break;
                default:
                    return null;
            }
            
            // Use local date strings to avoid UTC timezone issues
            const result = {
                startDate: getLocalDateString(startDate),
                endDate: getLocalDateString(endDate)
            };
            
            console.log('📅 Date filter:', filter, '→', result);
            return result;
        }
        
        function loadAllData() {
            loadStats();
            loadLeads();
            loadOverviewData();
            loadOverviewFunnelComparison();
            loadFunnelData();
            loadRecoveryData();
            loadRefunds();
            loadHeatmapData();
            loadPeriodComparison();
            loadSalesData();
            loadCountriesData();
        }
        
        // Global filter change handler (language + date + source)
        function onGlobalFilterChange() {
            console.log('🔄 Global filter changed');
            const globalLangFilter = document.getElementById('globalFunnelFilter')?.value || '';
            const globalDateFilter = document.getElementById('globalDateFilter')?.value || 'all';
            const globalSourceFilter = document.getElementById('globalSourceFilter')?.value || '';
            console.log('📊 Language:', globalLangFilter, 'Date:', globalDateFilter, 'Source:', globalSourceFilter);
            
            // Sync with leads page filter
            const languageFilter = document.getElementById('languageFilter');
            if (languageFilter) languageFilter.value = globalLangFilter;
            
            // Reload all data
            loadAllData();
        }
        
        // Get global source filter value
        function getGlobalSourceFilter() {
            return document.getElementById('globalSourceFilter')?.value || '';
        }
        
        // Load funnel comparison data for Overview (EN vs ES stats in header)
        async function loadOverviewFunnelComparison() {
            try {
                const dateRange = getGlobalDateRange();
                const globalSource = getGlobalSourceFilter();
                
                // Build date params string for reuse
                let dateParams = '';
                if (dateRange) {
                    dateParams = `&startDate=${dateRange.startDate}&endDate=${dateRange.endDate}`;
                }
                let sourceParam = globalSource ? `&source=${globalSource}` : '';
                
                let leadsUrl = `${API_URL}/api/admin/leads?limit=10000${dateParams}`;
                
                // Fetch leads + sales for EN and ES + breakdown by source - all with date filters
                const [leadsRes, enSalesRes, esSalesRes, enMainSalesRes, enAffSalesRes, esMainSalesRes, esAffSalesRes] = await Promise.all([
                    fetch(leadsUrl, { headers: { 'Authorization': `Bearer ${authToken}` } }),
                    fetch(`${API_URL}/api/admin/sales?language=en${dateParams}${sourceParam}`, { headers: { 'Authorization': `Bearer ${authToken}` } }).catch(() => null),
                    fetch(`${API_URL}/api/admin/sales?language=es${dateParams}${sourceParam}`, { headers: { 'Authorization': `Bearer ${authToken}` } }).catch(() => null),
                    fetch(`${API_URL}/api/admin/sales?language=en&source=main${dateParams}`, { headers: { 'Authorization': `Bearer ${authToken}` } }).catch(() => null),
                    fetch(`${API_URL}/api/admin/sales?language=en&source=affiliate${dateParams}`, { headers: { 'Authorization': `Bearer ${authToken}` } }).catch(() => null),
                    fetch(`${API_URL}/api/admin/sales?language=es&source=main${dateParams}`, { headers: { 'Authorization': `Bearer ${authToken}` } }).catch(() => null),
                    fetch(`${API_URL}/api/admin/sales?language=es&source=affiliate${dateParams}`, { headers: { 'Authorization': `Bearer ${authToken}` } }).catch(() => null)
                ]);
                
                if (leadsRes.status === 401 || leadsRes.status === 403) { logout(); return; }
                
                const data = await leadsRes.json();
                const enSales = enSalesRes?.ok ? await enSalesRes.json() : { approved: 0, revenue: 0 };
                const esSales = esSalesRes?.ok ? await esSalesRes.json() : { approved: 0, revenue: 0 };
                const enMainSales = enMainSalesRes?.ok ? await enMainSalesRes.json() : { approved: 0 };
                const enAffSales = enAffSalesRes?.ok ? await enAffSalesRes.json() : { approved: 0 };
                const esMainSales = esMainSalesRes?.ok ? await esMainSalesRes.json() : { approved: 0 };
                const esAffSales = esAffSalesRes?.ok ? await esAffSalesRes.json() : { approved: 0 };
                
                const todayStart = new Date();
                todayStart.setHours(0, 0, 0, 0);
                
                let enToday = 0, esToday = 0;
                let enConverted = 0, esConverted = 0;
                let enTotal = 0, esTotal = 0;
                
                (data.leads || []).forEach(lead => {
                    const leadDate = new Date(lead.created_at);
                    const lang = lead.funnel_language || 'en';
                    
                    if (lang === 'en') {
                        enTotal++;
                        if (leadDate >= todayStart) enToday++;
                        if (lead.status === 'converted') enConverted++;
                    } else if (lang === 'es') {
                        esTotal++;
                        if (leadDate >= todayStart) esToday++;
                        if (lead.status === 'converted') esConverted++;
                    }
                });
                
                // Update EN stats
                const setEl = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
                setEl('enLeadsTotal', enTotal.toLocaleString('pt-BR'));
                setEl('enLeadsToday', enToday.toLocaleString('pt-BR'));
                const enConvRate = enTotal > 0 ? (((enSales.approved || 0) / enTotal) * 100).toFixed(1) : 0;
                setEl('enConvRate', enConvRate + '%');
                setEl('enRevenue', 'R$ ' + (enSales.revenue || 0).toLocaleString('pt-BR', { minimumFractionDigits: 0 }));
                
                // Update ES stats
                setEl('esLeadsTotal', esTotal.toLocaleString('pt-BR'));
                setEl('esLeadsToday', esToday.toLocaleString('pt-BR'));
                const esConvRate = esTotal > 0 ? (((esSales.approved || 0) / esTotal) * 100).toFixed(1) : 0;
                setEl('esConvRate', esConvRate + '%');
                setEl('esRevenue', 'R$ ' + (esSales.revenue || 0).toLocaleString('pt-BR', { minimumFractionDigits: 0 }));
                
                // Update source breakdown
                setEl('enSalesMain', (enMainSales.approved || 0) + ' vendas');
                setEl('enSalesAff', (enAffSales.approved || 0) + ' vendas');
                setEl('esSalesMain', (esMainSales.approved || 0) + ' vendas');
                setEl('esSalesAff', (esAffSales.approved || 0) + ' vendas');
                
            } catch (error) {
                console.error('Error loading overview funnel comparison:', error);
            }
        }
        
        
        function switchTab(tabName) {
            document.querySelectorAll('.nav-item').forEach(item => item.classList.remove('active'));
            document.querySelector(`.nav-item[onclick="switchTab('${tabName}')"]`)?.classList.add('active');
            document.querySelectorAll('.tab-content').forEach(tab => tab.classList.remove('active'));
            document.getElementById(`tab-${tabName}`).classList.add('active');
            
            const pageInfo = pageTitles[tabName];
            if (pageInfo) {
                document.getElementById('pageTitle').textContent = pageInfo.title;
                document.getElementById('pageSubtitle').textContent = pageInfo.subtitle;
            }
            
            if (tabName === 'funnel') loadFunnelData();
            else if (tabName === 'sales') loadSalesData();
            else if (tabName === 'recovery') loadRecoveryData();
            else if (tabName === 'refunds') loadRefunds();
            else if (tabName === 'countries') loadCountriesData();
            else if (tabName === 'reports') loadReportsStats();
            else if (tabName === 'settings') {
                document.getElementById('postbackUrl').textContent = `${API_URL}/api/postback/monetizze`;
                // Initialize sync dates with today (local timezone)
                const today = getLocalDateString();
                document.getElementById('syncStartDate').value = today;
                document.getElementById('syncEndDate').value = today;
            }
            else if (tabName === 'users') loadUsers();
        }
        
        function logout() {
            localStorage.removeItem('adminToken');
            localStorage.removeItem('adminUser');
            location.reload();
        }
        
        // Store current user info
        let currentUser = null;
        
        // Get current user from token
        function getCurrentUser() {
            const userStr = localStorage.getItem('adminUser');
            if (userStr) {
                try {
                    return JSON.parse(userStr);
                } catch(e) {}
            }
            return { role: 'admin', name: 'Admin' }; // Default fallback
        }
        
        // Update UI based on user role
        function updateUIForRole(user) {
            currentUser = user;
            
            // Update sidebar
            const avatar = document.getElementById('userAvatarSidebar');
            const nameEl = document.getElementById('userNameSidebar');
            
            if (avatar && user.name) {
                avatar.textContent = user.name.charAt(0).toUpperCase();
            }
            if (nameEl && user.name) {
                nameEl.textContent = user.name;
            }
            
            // Show/hide admin-only elements
            const navUsers = document.getElementById('navUsers');
            if (navUsers) {
                navUsers.style.display = user.role === 'admin' ? 'flex' : 'none';
            }
            
            // Hide danger zone for non-admins
            const dangerZone = document.querySelector('.config-box[style*="border-color: var(--danger)"]');
            if (dangerZone && user.role !== 'admin') {
                dangerZone.style.display = 'none';
            }
        }
        
        // ==================== USER MANAGEMENT ====================
        
        async function loadUsers() {
            try {
                const response = await fetch(`${API_URL}/api/admin/users`, {
                    headers: { 'Authorization': `Bearer ${token}` }
                });
                
                if (response.status === 403) {
                    showToast('Acesso Negado', 'Você não tem permissão para ver usuários', 'error');
                    return;
                }
                
                const data = await response.json();
                renderUsers(data.users || []);
            } catch (error) {
                console.error('Error loading users:', error);
            }
        }
        
        function renderUsers(users) {
            const grid = document.getElementById('usersGrid');
            const emptyState = document.getElementById('usersEmptyState');
            
            if (!grid) return;
            
            // Update stats
            const stats = {
                admins: users.filter(u => u.role === 'admin').length,
                support: users.filter(u => u.role === 'support').length,
                active: users.filter(u => u.is_active).length,
                total: users.length
            };
            
            document.getElementById('statsAdmins').textContent = stats.admins;
            document.getElementById('statsSupport').textContent = stats.support;
            document.getElementById('statsActive').textContent = stats.active;
            document.getElementById('statsTotal').textContent = stats.total;
            
            if (users.length === 0) {
                grid.style.display = 'none';
                if (emptyState) emptyState.style.display = 'block';
                return;
            }
            
            grid.style.display = 'grid';
            if (emptyState) emptyState.style.display = 'none';
            
            const roleLabels = {
                'admin': { icon: '🔐', label: 'Admin', color: '#ef4444', gradient: 'linear-gradient(135deg, #ef4444, #dc2626)' },
                'support': { icon: '💬', label: 'Suporte', color: '#3b82f6', gradient: 'linear-gradient(135deg, #3b82f6, #2563eb)' },
                'viewer': { icon: '👁️', label: 'Visualizador', color: '#22c55e', gradient: 'linear-gradient(135deg, #22c55e, #16a34a)' }
            };
            
            grid.innerHTML = users.map(user => {
                const role = roleLabels[user.role] || roleLabels.viewer;
                const isCurrentUser = currentUser && (currentUser.id === user.id || currentUser.email === user.email);
                const isMainAdmin = user.id === 1;
                const lastLogin = user.last_login ? new Date(user.last_login) : null;
                const createdAt = new Date(user.created_at);
                
                return `
                    <div class="stat-card" style="position: relative; overflow: hidden; border: 1px solid ${user.is_active ? 'var(--border)' : 'rgba(239, 68, 68, 0.3)'}; ${isCurrentUser ? 'box-shadow: 0 0 0 2px rgba(34, 197, 94, 0.3);' : ''}">
                        ${isCurrentUser ? `<div style="position: absolute; top: 12px; right: 12px; background: var(--success); color: white; padding: 4px 10px; border-radius: 6px; font-size: 11px; font-weight: 600; z-index: 2;">VOCÊ</div>` : ''}
                        
                        <!-- Avatar & Name -->
                        <div style="display: flex; align-items: start; gap: 16px; margin-bottom: 20px;">
                            <div style="width: 64px; height: 64px; border-radius: 16px; background: ${role.gradient}; display: flex; align-items: center; justify-content: center; font-size: 28px; font-weight: 700; color: white; flex-shrink: 0; box-shadow: 0 4px 12px ${role.color}40;">
                                ${(user.name || user.username || 'U').charAt(0).toUpperCase()}
                            </div>
                            <div style="flex: 1; min-width: 0;">
                                <h3 style="margin: 0 0 4px 0; color: var(--text-primary); font-size: 18px; font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
                                    ${user.name || user.username}
                                </h3>
                                <div style="font-size: 13px; color: var(--text-muted); margin-bottom: 8px;">@${user.username}</div>
                                <div style="display: inline-flex; align-items: center; gap: 6px; padding: 4px 10px; border-radius: 6px; background: ${role.color}15; border: 1px solid ${role.color}30;">
                                    <span style="font-size: 14px;">${role.icon}</span>
                                    <span style="font-size: 12px; font-weight: 600; color: ${role.color};">${role.label}</span>
                                </div>
                            </div>
                        </div>
                        
                        <!-- Email -->
                        <div style="display: flex; align-items: center; gap: 8px; padding: 10px 12px; background: var(--bg-tertiary); border-radius: 8px; margin-bottom: 12px;">
                            <span style="font-size: 14px;">📧</span>
                            <span style="font-size: 13px; color: var(--text-secondary); overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">${user.email}</span>
                        </div>
                        
                        <!-- Status & Info -->
                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 16px;">
                            <div style="padding: 8px; background: var(--bg-tertiary); border-radius: 6px;">
                                <div style="font-size: 11px; color: var(--text-muted); margin-bottom: 4px;">STATUS</div>
                                <div style="font-size: 13px; font-weight: 600; color: ${user.is_active ? 'var(--success)' : 'var(--danger)'};">
                                    ${user.is_active ? '✅ Ativo' : '🚫 Inativo'}
                                </div>
                            </div>
                            <div style="padding: 8px; background: var(--bg-tertiary); border-radius: 6px;">
                                <div style="font-size: 11px; color: var(--text-muted); margin-bottom: 4px;">MEMBRO DESDE</div>
                                <div style="font-size: 13px; font-weight: 600; color: var(--text-primary);">
                                    ${createdAt.toLocaleDateString('pt-BR', { day: '2-digit', month: 'short' })}
                                </div>
                            </div>
                        </div>
                        
                        <!-- Last Login -->
                        <div style="padding: 10px 12px; background: var(--bg-tertiary); border-radius: 8px; margin-bottom: 16px;">
                            <div style="font-size: 11px; color: var(--text-muted); margin-bottom: 4px;">ÚLTIMO ACESSO</div>
                            <div style="font-size: 13px; color: var(--text-secondary);">
                                ${lastLogin ? `${lastLogin.toLocaleDateString('pt-BR')} às ${lastLogin.toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' })}` : 'Nunca acessou'}
                            </div>
                        </div>
                        
                        <!-- Actions -->
                        <div style="display: flex; gap: 8px;">
                            <button 
                                class="btn-secondary" 
                                onclick="openEditUserModal(${user.id}, '${(user.name || '').replace(/'/g, "\\'")}', '${user.role}', ${user.is_active})" 
                                style="flex: 1; padding: 10px; font-size: 13px; font-weight: 600;"
                                ${isMainAdmin && !isCurrentUser ? 'disabled' : ''}
                            >
                                ✏️ Editar
                            </button>
                            <button 
                                class="btn-secondary" 
                                onclick="deleteUser(${user.id}, '${(user.name || user.username).replace(/'/g, "\\'")}')  " 
                                style="padding: 10px; background: rgba(239, 68, 68, 0.1); border-color: rgba(239, 68, 68, 0.3); color: var(--danger);"
                                ${isMainAdmin || isCurrentUser ? 'disabled style="opacity: 0.3; cursor: not-allowed;"' : ''}
                                title="${isCurrentUser ? 'Você não pode excluir sua própria conta' : isMainAdmin ? 'Admin principal não pode ser excluído' : 'Excluir usuário'}"
                            >
                                🗑️
                            </button>
                        </div>
                    </div>
                `;
            }).join('');
        }
        
        function openAddUserModal() {
            document.getElementById('addUserForm').reset();
            document.getElementById('addUserModal').classList.add('active');
        }
        
        function closeAddUserModal() {
            document.getElementById('addUserModal').classList.remove('active');
        }
        
        async function createUser(e) {
            e.preventDefault();
            
            const name = document.getElementById('newUserName').value.trim();
            const username = document.getElementById('newUserUsername').value.trim().toLowerCase();
            const email = document.getElementById('newUserEmail').value.trim().toLowerCase();
            const password = document.getElementById('newUserPassword').value;
            const role = document.getElementById('newUserRole').value;
            
            try {
                const response = await fetch(`${API_URL}/api/admin/users`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': `Bearer ${token}`
                    },
                    body: JSON.stringify({ username, email, password, name, role })
                });
                
                const data = await response.json();
                
                if (!response.ok) {
                    showToast('Erro', data.error || 'Falha ao criar usuário', 'error');
                    return false;
                }
                
                showToast('Sucesso!', `Usuário ${username} criado`, 'success');
                closeAddUserModal();
                loadUsers();
                return false;
                
            } catch (error) {
                console.error('Error creating user:', error);
                showToast('Erro', 'Falha ao criar usuário', 'error');
                return false;
            }
        }
        
        function openEditUserModal(id, name, role, isActive) {
            document.getElementById('editUserId').value = id;
            document.getElementById('editUserName').value = name;
            document.getElementById('editUserRole').value = role;
            document.getElementById('editUserStatus').value = isActive.toString();
            document.getElementById('editUserPassword').value = '';
            document.getElementById('editUserModal').classList.add('active');
        }
        
        function closeEditUserModal() {
            document.getElementById('editUserModal').classList.remove('active');
        }
        
        async function saveUserEdit() {
            const id = document.getElementById('editUserId').value;
            const name = document.getElementById('editUserName').value.trim();
            const role = document.getElementById('editUserRole').value;
            const is_active = document.getElementById('editUserStatus').value === 'true';
            const password = document.getElementById('editUserPassword').value;
            
            const body = { name, role, is_active };
            if (password) body.password = password;
            
            try {
                const response = await fetch(`${API_URL}/api/admin/users/${id}`, {
                    method: 'PUT',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': `Bearer ${token}`
                    },
                    body: JSON.stringify(body)
                });
                
                const data = await response.json();
                
                if (!response.ok) {
                    showToast('Erro', data.error || 'Falha ao atualizar usuário', 'error');
                    return;
                }
                
                showToast('Sucesso!', 'Usuário atualizado', 'success');
                closeEditUserModal();
                loadUsers();
                
            } catch (error) {
                console.error('Error updating user:', error);
                showToast('Erro', 'Falha ao atualizar usuário', 'error');
            }
        }
        
        async function deleteUser(id, name) {
            if (!confirm(`⚠️ Tem certeza que deseja excluir o usuário "${name}"?\n\nEsta ação não pode ser desfeita.`)) {
                return;
            }
            
            try {
                const response = await fetch(`${API_URL}/api/admin/users/${id}`, {
                    method: 'DELETE',
                    headers: { 'Authorization': `Bearer ${token}` }
                });
                
                const data = await response.json();
                
                if (!response.ok) {
                    showToast('Erro', data.error || 'Falha ao excluir usuário', 'error');
                    return;
                }
                
                showToast('Sucesso!', 'Usuário excluído', 'success');
                loadUsers();
                
            } catch (error) {
                console.error('Error deleting user:', error);
                showToast('Erro', 'Falha ao excluir usuário', 'error');
            }
        }
        
        function refreshData() {
            showToast('Atualizando...', 'Carregando dados mais recentes', 'success');
            loadAllData();
        }
        
        // Toast notifications
        function showToast(title, message, type = 'success') {
            const container = document.getElementById('toastContainer');
            const icons = { success: '✅', warning: '⚠️', error: '❌' };
            const toast = document.createElement('div');
            toast.className = `toast ${type}`;
            toast.innerHTML = `
                <span class="toast-icon">${icons[type]}</span>
                <div class="toast-content">
                    <div class="toast-title">${title}</div>
                    <div class="toast-message">${message}</div>
                </div>
                <button class="toast-close" onclick="this.parentElement.remove()">×</button>
            `;
            container.appendChild(toast);
            setTimeout(() => toast.remove(), 5000);
        }
        
        // Charts
        function initCharts() {
            const ctxLeads = document.getElementById('leadsChart')?.getContext('2d');
            const ctxStatus = document.getElementById('statusChart')?.getContext('2d');
            
            if (ctxLeads) {
                leadsChart = new Chart(ctxLeads, {
                    type: 'line',
                    data: { labels: [], datasets: [{ label: 'Leads', data: [], borderColor: '#10b981', backgroundColor: 'rgba(16, 185, 129, 0.1)', fill: true, tension: 0.4 }] },
                    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#71717a' } }, y: { grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#71717a' } } } }
                });
            }
            
            if (ctxStatus) {
                statusChart = new Chart(ctxStatus, {
                    type: 'doughnut',
                    data: { labels: ['Novos', 'Contatados', 'Convertidos', 'Perdidos'], datasets: [{ data: [0, 0, 0, 0], backgroundColor: ['#3b82f6', '#f59e0b', '#22c55e', '#ef4444'], borderWidth: 0 }] },
                    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom', labels: { color: '#a1a1aa', padding: 16 } } } }
                });
            }
        }
        
        async function loadOverviewData() {
            try {
                // Build query params from global filters
                const dateRange = getGlobalDateRange();
                const globalLang = document.getElementById('globalFunnelFilter')?.value || '';
                const globalSource = getGlobalSourceFilter();
                
                let statsParams = new URLSearchParams();
                let salesParams = new URLSearchParams();
                
                if (dateRange) {
                    statsParams.set('startDate', dateRange.startDate);
                    statsParams.set('endDate', dateRange.endDate);
                    salesParams.set('startDate', dateRange.startDate);
                    salesParams.set('endDate', dateRange.endDate);
                }
                if (globalLang) {
                    statsParams.set('language', globalLang);
                    salesParams.set('language', globalLang);
                }
                if (globalSource) {
                    salesParams.set('source', globalSource);
                }
                
                const statsUrl = `${API_URL}/api/admin/stats${statsParams.toString() ? '?' + statsParams.toString() : ''}`;
                const salesUrl = `${API_URL}/api/admin/sales${salesParams.toString() ? '?' + salesParams.toString() : ''}`;
                
                const [statsRes, salesRes] = await Promise.all([
                    fetch(statsUrl, { headers: { 'Authorization': `Bearer ${authToken}` } }),
                    fetch(salesUrl, { headers: { 'Authorization': `Bearer ${authToken}` } }).catch(() => null)
                ]);
                
                if (statsRes.status === 401) { logout(); return; }
                
                const stats = await statsRes.json();
                if (stats.error) { console.error('Stats API error:', stats.error); return; }
                const sales = salesRes?.ok ? await salesRes.json() : { approved: 0, revenue: 0, today: 0 };
                
                const revenueTotal = sales.revenue || 0;
                const approvedTotal = sales.approved || 0;
                
                // === MAIN KPIs ===
                const setEl = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
                
                // Leads
                setEl('overviewTotal', stats.total.toLocaleString('pt-BR'));
                
                // Sales
                setEl('overviewTotalSales', approvedTotal.toLocaleString('pt-BR'));
                
                // Revenue
                setEl('overviewRevenue', 'R$ ' + revenueTotal.toLocaleString('pt-BR', { minimumFractionDigits: 2 }));
                
                // Conversion rate
                const rate = stats.total > 0 ? ((approvedTotal / stats.total) * 100).toFixed(1) : 0;
                const convEl = document.getElementById('overviewConverted');
                if (convEl) {
                    convEl.textContent = `${rate}%`;
                    convEl.style.color = rate > 5 ? '#10b981' : rate > 2 ? '#f59e0b' : '#ef4444';
                }
                setEl('overviewConvRate', `${approvedTotal} vendas / ${stats.total} leads`);
                
                // Abandoned
                const converted = stats.byStatus?.find(s => s.status === 'converted')?.count || 0;
                const abandoned = Math.max(0, stats.total - converted - (stats.byStatus?.find(s => s.status === 'contacted')?.count || 0));
                setEl('overviewAbandoned', abandoned.toLocaleString('pt-BR'));
                
                const abandonRateEl = document.getElementById('overviewAbandonRate');
                if (abandonRateEl && stats.total > 0) {
                    const abandonRate = ((abandoned / stats.total) * 100).toFixed(1);
                    abandonRateEl.textContent = `${abandonRate}% do total`;
                }
                
                // Update last update time
                const now = new Date();
                const timeStr = now.toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' });
                setEl('overviewTodayTime', `Atualizado ${timeStr}`);
                
                // Update quick action texts
                setEl('quickLeadsText', `${stats.today} novos leads`);
                setEl('quickRecoveryText', `${abandoned} leads aguardando`);
                setEl('quickSalesText', 'R$ ' + revenueTotal.toLocaleString('pt-BR', { minimumFractionDigits: 2 }));
                setEl('navRecoveryCount', abandoned);
                
                // Sparkline
                if (stats.byDay) {
                    const data = stats.byDay.map(d => parseInt(d.count)).reverse();
                    createSparkline(data.slice(-7), 'sparklineLeads');
                }
                
                // Change vs yesterday
                if (stats.byDay && stats.byDay.length >= 2) {
                    const todayCount = stats.today || 0;
                    const yesterdayData = stats.byDay.find(d => {
                        const dateStr = new Date(d.date).toLocaleDateString('pt-BR');
                        const yesterday = new Date();
                        yesterday.setDate(yesterday.getDate() - 1);
                        return dateStr === yesterday.toLocaleDateString('pt-BR');
                    });
                    const yesterdayCount = yesterdayData ? parseInt(yesterdayData.count) : 0;
                    const changeEl = document.getElementById('overviewTotalChange');
                    if (changeEl) {
                        if (yesterdayCount > 0) {
                            const pct = (((todayCount - yesterdayCount) / yesterdayCount) * 100).toFixed(0);
                            changeEl.textContent = `${pct >= 0 ? '↑' : '↓'} ${Math.abs(pct)}% vs ontem`;
                            changeEl.className = `stat-change ${pct >= 0 ? 'up' : 'down'}`;
                        } else {
                            changeEl.textContent = `↑ ${todayCount} novos hoje`;
                            changeEl.className = 'stat-change up';
                        }
                    }
                }
                
                // === CHARTS ===
                if (stats.byDay && leadsChart) {
                    const labels = stats.byDay.map(d => new Date(d.date).toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit' })).reverse();
                    const chartData = stats.byDay.map(d => parseInt(d.count)).reverse();
                    leadsChart.data.labels = labels;
                    leadsChart.data.datasets[0].data = chartData;
                    leadsChart.update();
                }
                
                if (stats.byStatus && statusChart) {
                    const newCount = stats.byStatus.find(s => s.status === 'new')?.count || 0;
                    const contactedCount = stats.byStatus.find(s => s.status === 'contacted')?.count || 0;
                    const convertedCount = stats.byStatus.find(s => s.status === 'converted')?.count || 0;
                    const lostCount = stats.byStatus.find(s => s.status === 'lost')?.count || 0;
                    statusChart.data.datasets[0].data = [newCount, contactedCount, convertedCount, lostCount];
                    statusChart.update();
                }
                
            } catch (error) {
                console.error('Error loading overview:', error);
            }
        }
        
        async function loadStats() {
            try {
                const dateRange = getGlobalDateRange();
                const globalLang = document.getElementById('globalFunnelFilter')?.value || '';
                
                let params = new URLSearchParams();
                if (dateRange) {
                    params.set('startDate', dateRange.startDate);
                    params.set('endDate', dateRange.endDate);
                }
                if (globalLang) {
                    params.set('language', globalLang);
                }
                
                let url = `${API_URL}/api/admin/stats${params.toString() ? '?' + params.toString() : ''}`;
                
                const response = await fetch(url, { headers: { 'Authorization': `Bearer ${authToken}` } });
                if (response.status === 401 || response.status === 403) { logout(); return; }
                const data = await response.json();
                
                const setEl = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
                setEl('statTotal', data.total.toLocaleString('pt-BR'));
                setEl('statToday', data.today.toLocaleString('pt-BR'));
                setEl('statWeek', data.thisWeek.toLocaleString('pt-BR'));
                setEl('navLeadCount', data.total);
                
                const converted = data.byStatus?.find(s => s.status === 'converted')?.count || 0;
                const rate = data.total > 0 ? ((converted / data.total) * 100).toFixed(1) : 0;
                setEl('statConversion', rate + '%');
            } catch (error) {
                console.error('Error loading stats:', error);
            }
        }
        
        async function loadLeads(page = 1) {
            currentPage = page;
            const search = document.getElementById('searchInput').value;
            const status = document.getElementById('statusFilter').value;
            const language = document.getElementById('languageFilter').value;
            const dateRange = getGlobalDateRange();
            
            // Update leads header indicator
            updateLeadsHeader(language);
            
            try {
                let url = `${API_URL}/api/admin/leads?page=${page}&limit=10`;
                if (search) url += `&search=${encodeURIComponent(search)}`;
                if (status) url += `&status=${status}`;
                if (language) url += `&language=${language}`;
                if (dateRange) {
                    url += `&startDate=${dateRange.startDate}&endDate=${dateRange.endDate}`;
                }
                
                const response = await fetch(url, { headers: { 'Authorization': `Bearer ${authToken}` } });
                if (response.status === 401 || response.status === 403) { logout(); return; }
                const data = await response.json();
                
                renderLeads(data.leads);
                renderRecentLeads(data.leads.slice(0, 5));
                renderPagination(data.pagination);
                
                // Update count with language indicator
                const langLabel = language === 'en' ? ' (Inglês)' : language === 'es' ? ' (Espanhol)' : '';
                document.getElementById('panelLeadCount').textContent = `${data.pagination.total} leads${langLabel}`;
            } catch (error) {
                console.error('Error loading leads:', error);
            }
        }
        
        function updateLeadsHeader(lang) {
            const indicator = document.getElementById('leadsLangIndicator');
            if (!indicator) return;
            if (lang === 'en') {
                indicator.innerHTML = '<span class="lang-badge en">🇺🇸 English</span>';
            } else if (lang === 'es') {
                indicator.innerHTML = '<span class="lang-badge es">🇪🇸 Español</span>';
            } else {
                indicator.innerHTML = '<span class="lang-badge all">🌍 Todos</span>';
            }
        }
        
        function renderLeads(leads) {
            if (!leads || leads.length === 0) {
                document.getElementById('leadsTableBody').innerHTML = `<tr><td colspan="10"><div class="empty-state"><div class="empty-state-icon">👥</div><h3>Nenhum lead encontrado</h3></div></td></tr>`;
                return;
            }
            
            let html = '';
            leads.forEach(lead => {
                const badgeClass = `badge-${lead.status || 'new'}`;
                const statusText = statusLabels[lead.status] || 'Novo';
                const tags = lead.notes?.match(/\[TAG:(\w+)\]/g)?.map(t => t.replace('[TAG:', '').replace(']', '')) || [];
                
                // Language/Funnel badge
                const lang = lead.funnel_language || 'en';
                const langBadge = lang === 'es' 
                    ? '<span class="badge" style="background: rgba(255,193,7,0.15); color: #ffc107; font-size: 11px;">🇪🇸 ES</span>'
                    : '<span class="badge" style="background: rgba(0,123,255,0.15); color: #448aff; font-size: 11px;">🇺🇸 EN</span>';
                
                // Country with flag
                const countryDisplay = lead.country_code 
                    ? `<span title="${lead.country || ''} - ${lead.city || ''}">${getCountryFlag(lead.country_code)} ${lead.country_code}</span>`
                    : '<span style="color: var(--text-muted);">-</span>';
                
                html += `<tr>
                    <td><strong>${lead.name || '-'}</strong></td>
                    <td class="text-primary">${lead.email}</td>
                    <td class="mono">${lead.whatsapp}</td>
                    <td>${langBadge}</td>
                    <td>${countryDisplay}</td>
                    <td class="mono">${lead.target_phone || '-'}</td>
                    <td>${tags.map(t => `<span class="tag ${t}">${t}</span>`).join('') || '-'}</td>
                    <td><span class="badge ${badgeClass}"><span class="badge-dot"></span>${statusText}</span></td>
                    <td>${formatDate(lead.created_at)}</td>
                    <td>
                        <button class="action-btn" onclick="openCustomerJourney(${lead.id})" title="Ver Jornada" style="background: rgba(102,126,234,0.15);">🛤️</button>
                        <button class="action-btn whatsapp" onclick="openWhatsAppModal('${lead.whatsapp}')" title="WhatsApp">📱</button>
                        <button class="action-btn" onclick="editLead(${lead.id}, '${lead.status || 'new'}', '${(lead.notes || '').replace(/'/g, "\\'")}')" title="Editar">✏️</button>
                    </td>
                </tr>`;
            });
            document.getElementById('leadsTableBody').innerHTML = html;
        }
        
        function renderRecentLeads(leads) {
            if (!leads || leads.length === 0) {
                document.getElementById('recentLeadsTable').innerHTML = `<tr><td colspan="5"><div class="empty-state"><h3>Nenhum lead ainda</h3></div></td></tr>`;
                return;
            }
            
            let html = '';
            leads.forEach(lead => {
                const badgeClass = `badge-${lead.status || 'new'}`;
                const statusText = statusLabels[lead.status] || 'Novo';
                const lang = lead.funnel_language || 'en';
                const langBadge = lang === 'es' 
                    ? '<span class="mini-lang-badge es">🇪🇸 ES</span>'
                    : '<span class="mini-lang-badge en">🇺🇸 EN</span>';
                html += `<tr>
                    <td class="text-primary">${lead.email}</td>
                    <td class="mono">${lead.whatsapp}</td>
                    <td>${langBadge}</td>
                    <td><span class="badge ${badgeClass}"><span class="badge-dot"></span>${statusText}</span></td>
                    <td>${timeAgo(lead.created_at)}</td>
                </tr>`;
            });
            document.getElementById('recentLeadsTable').innerHTML = html;
        }
        
        function renderPagination(pagination) {
            const { page, total, totalPages } = pagination;
            const start = (page - 1) * 10 + 1;
            const end = Math.min(page * 10, total);
            document.getElementById('paginationInfo').textContent = total > 0 ? `Mostrando ${start} - ${end} de ${total}` : 'Mostrando 0 de 0';
            
            let buttons = '';
            if (totalPages > 1) {
                buttons += `<button class="page-btn" onclick="loadLeads(${page - 1})" ${page === 1 ? 'disabled' : ''}>←</button>`;
                for (let i = 1; i <= Math.min(totalPages, 5); i++) {
                    buttons += `<button class="page-btn ${i === page ? 'active' : ''}" onclick="loadLeads(${i})">${i}</button>`;
                }
                buttons += `<button class="page-btn" onclick="loadLeads(${page + 1})" ${page === totalPages ? 'disabled' : ''}>→</button>`;
            }
            document.getElementById('paginationButtons').innerHTML = buttons;
        }
        
        function formatDate(dateStr) {
            if (!dateStr) return '-';
            return new Date(dateStr).toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' });
        }
        
        function timeAgo(dateStr) {
            if (!dateStr) return '-';
            const seconds = Math.floor((new Date() - new Date(dateStr)) / 1000);
            if (seconds < 60) return 'Agora';
            if (seconds < 3600) return `${Math.floor(seconds / 60)}min atrás`;
            if (seconds < 86400) return `${Math.floor(seconds / 3600)}h atrás`;
            return `${Math.floor(seconds / 86400)}d atrás`;
        }
        
        // Search and filter
        document.getElementById('searchInput').addEventListener('input', () => {
            clearTimeout(searchTimeout);
            searchTimeout = setTimeout(() => loadLeads(1), 300);
        });
        document.getElementById('statusFilter').addEventListener('change', () => loadLeads(1));
        document.getElementById('languageFilter').addEventListener('change', () => {
            // Sync with global filter
            const langValue = document.getElementById('languageFilter').value;
            const globalFilter = document.getElementById('globalFunnelFilter');
            if (globalFilter) globalFilter.value = langValue;
            loadLeads(1);
        });
        
        // Edit lead
        function editLead(id, status, notes) {
            document.getElementById('editLeadId').value = id;
            document.getElementById('editStatus').value = status;
            document.getElementById('editNotes').value = notes.replace(/\\'/g, "'").replace(/\[TAG:\w+\]/g, '').trim();
            
            // Parse existing tags
            selectedTags = notes.match(/\[TAG:(\w+)\]/g)?.map(t => t.replace('[TAG:', '').replace(']', '')) || [];
            document.querySelectorAll('#editModal .tag').forEach(tag => {
                const tagType = tag.getAttribute('onclick').match(/'(\w+)'/)[1];
                tag.classList.toggle('active', selectedTags.includes(tagType));
            });
            
            document.getElementById('editModal').classList.add('active');
        }
        
        function toggleTag(el, tag) {
            el.classList.toggle('active');
            if (selectedTags.includes(tag)) {
                selectedTags = selectedTags.filter(t => t !== tag);
            } else {
                selectedTags.push(tag);
            }
        }
        
        function closeEditModal() { document.getElementById('editModal').classList.remove('active'); }
        
        async function saveLeadEdit() {
            const id = document.getElementById('editLeadId').value;
            const status = document.getElementById('editStatus').value;
            let notes = document.getElementById('editNotes').value;
            
            // Append tags to notes
            selectedTags.forEach(tag => { notes += ` [TAG:${tag}]`; });
            
            try {
                const response = await fetch(`${API_URL}/api/admin/leads/${id}`, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${authToken}` },
                    body: JSON.stringify({ status, notes })
                });
                
                if (response.ok) {
                    closeEditModal();
                    loadLeads(currentPage);
                    loadStats();
                    showToast('Lead atualizado!', 'As alterações foram salvas', 'success');
                }
            } catch (error) {
                showToast('Erro', 'Não foi possível salvar', 'error');
            }
        }
        
        // WhatsApp
        function openWhatsAppModal(phone) {
            document.getElementById('whatsappPhone').value = phone;
            document.getElementById('whatsappModal').classList.add('active');
        }
        
        function closeWhatsAppModal() { document.getElementById('whatsappModal').classList.remove('active'); }
        
        function sendWhatsApp() {
            const phone = document.getElementById('whatsappPhone').value.replace(/\D/g, '');
            const message = encodeURIComponent(document.getElementById('whatsappMessage').value);
            window.open(`https://wa.me/${phone}?text=${message}`, '_blank');
            closeWhatsAppModal();
            showToast('WhatsApp aberto!', 'A conversa foi iniciada', 'success');
        }
        
        function selectTemplate(id) {
            document.querySelectorAll('.template-item').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.template-item')[id - 1].classList.add('active');
            document.getElementById('whatsappMessage').value = templates[id];
        }
        
        // Recovery
        async function loadRecoveryData() {
            try {
                const globalFilter = document.getElementById('globalFunnelFilter')?.value || '';
                const dateRange = getGlobalDateRange();
                
                // Build query params
                let params = ['status=new', 'limit=50'];
                if (globalFilter) params.push(`language=${globalFilter}`);
                if (dateRange) {
                    params.push(`startDate=${dateRange.startDate}`);
                    params.push(`endDate=${dateRange.endDate}`);
                }
                const queryString = '?' + params.join('&');
                
                // Update recovery header indicator
                updateRecoveryHeader(globalFilter);
                
                const response = await fetch(`${API_URL}/api/admin/leads${queryString}`, { headers: { 'Authorization': `Bearer ${authToken}` } });
                if (response.status === 401 || response.status === 403) { logout(); return; }
                const data = await response.json();
                
                const leads = data.leads || [];
                document.getElementById('recoveryAbandonedCount').textContent = leads.length;
                document.getElementById('recoveryPendingCount').textContent = leads.filter(l => !l.notes?.includes('[CONTACTED]')).length;
                document.getElementById('recoveryRecoveredCount').textContent = leads.filter(l => l.status === 'converted').length;
                document.getElementById('recoveryLeadCount').textContent = `${leads.length} leads`;
                
                if (leads.length === 0) {
                    document.getElementById('recoveryTableBody').innerHTML = `<tr><td colspan="6"><div class="empty-state"><div class="empty-state-icon">🎉</div><h3>Nenhum abandono!</h3><p>Todos os leads foram acompanhados.</p></div></td></tr>`;
                    return;
                }
                
                let html = '';
                leads.forEach(lead => {
                    const attempts = (lead.notes?.match(/\[CONTACTED\]/g) || []).length;
                    html += `<tr>
                        <td class="text-primary">${lead.email}</td>
                        <td class="mono">${lead.whatsapp}</td>
                        <td><span class="tag abandoned">Checkout</span></td>
                        <td>${timeAgo(lead.created_at)}</td>
                        <td>${attempts} tentativas</td>
                        <td>
                            <button class="action-btn whatsapp" onclick="sendRecoveryMessage('${lead.whatsapp}')" title="Enviar">📱</button>
                        </td>
                    </tr>`;
                });
                document.getElementById('recoveryTableBody').innerHTML = html;
                
            } catch (error) {
                console.error('Error loading recovery:', error);
            }
        }
        
        function sendRecoveryMessage(phone) {
            openWhatsAppModal(phone);
        }
        
        function sendBulkRecovery() {
            showToast('Função em desenvolvimento', 'Disponível em breve!', 'warning');
        }
        
        // Funnel
        async function loadFunnelData() {
            try {
                // Use the funnel-specific filter instead of global filter
                const funnelFilter = document.getElementById('funnelLanguageFilter')?.value || '';
                
                if (!funnelFilter) {
                    // No filter = show comparison view with both funnels side by side
                    await loadFunnelComparison();
                } else {
                    // Specific filter = show single funnel view
                    await loadSingleFunnel(funnelFilter);
                }
            } catch (error) {
                console.error('Error loading funnel:', error);
            }
        }
        
        async function loadFunnelComparison() {
            // Show comparison view, hide single view
            document.getElementById('funnelComparisonView').style.display = 'block';
            document.getElementById('funnelSingleView').style.display = 'none';
            
            try {
                // Load all funnel data first (without language filter)
                const allResponse = await fetch(`${API_URL}/api/admin/funnel`, { 
                    headers: { 'Authorization': `Bearer ${authToken}` } 
                });
                
                if (allResponse.status === 401) { logout(); return; }
                
                const allData = await allResponse.json();
                
                // For now, show all data in EN column (since most legacy data is English)
                // and show a message for ES that it needs new data
                renderFunnelStepsForLanguage(allData.funnelStats, 'funnelStepsEN', '#3b82f6');
                const allStats = calculateFunnelMetrics(allData.funnelStats);
                document.getElementById('funnelVisitorsEN').textContent = allStats.visitors.toLocaleString('pt-BR');
                document.getElementById('funnelConvEN').textContent = allStats.conversionRate + '%';
                
                // Try to load ES separately
                try {
                    const esResponse = await fetch(`${API_URL}/api/admin/funnel?language=es`, { 
                        headers: { 'Authorization': `Bearer ${authToken}` } 
                    });
                    const esData = await esResponse.json();
                    
                    if (esData.funnelStats && esData.funnelStats.length > 0) {
                        renderFunnelStepsForLanguage(esData.funnelStats, 'funnelStepsES', '#f59e0b');
                        const esStats = calculateFunnelMetrics(esData.funnelStats);
                        document.getElementById('funnelVisitorsES').textContent = esStats.visitors.toLocaleString('pt-BR');
                        document.getElementById('funnelConvES').textContent = esStats.conversionRate + '%';
                    } else {
                        document.getElementById('funnelStepsES').innerHTML = '<div style="text-align: center; padding: 20px; color: var(--text-muted);"><p>📊 Sem dados do funil espanhol ainda.</p><p style="font-size: 12px;">Os dados aparecerão após novos visitantes acessarem o funil em espanhol.</p></div>';
                        document.getElementById('funnelVisitorsES').textContent = '0';
                        document.getElementById('funnelConvES').textContent = '0.0%';
                    }
                } catch (esError) {
                    console.log('ES funnel data not available:', esError);
                    document.getElementById('funnelStepsES').innerHTML = '<div style="text-align: center; padding: 20px; color: var(--text-muted);">Sem dados ES</div>';
                }
                
                // Show all journeys
                renderJourneys(allData.journeys || [], true);
                
            } catch (error) {
                console.error('Error loading funnel comparison:', error);
            }
        }
        
        async function loadSingleFunnel(language) {
            // Show single view, hide comparison view
            document.getElementById('funnelComparisonView').style.display = 'none';
            document.getElementById('funnelSingleView').style.display = 'block';
            
            try {
                const response = await fetch(`${API_URL}/api/admin/funnel?language=${language}`, { 
                    headers: { 'Authorization': `Bearer ${authToken}` } 
                });
                if (response.status === 401 || response.status === 403) { logout(); return; }
                const data = await response.json();
                
                renderFunnelStats(data.funnelStats);
                renderDailyStats(data.dailyStats);
                renderJourneys(data.journeys.map(j => ({ ...j, language })), true);
            } catch (error) {
                console.error('Error loading single funnel:', error);
            }
        }
        
        function calculateFunnelMetrics(stats) {
            const statsMap = {};
            (stats || []).forEach(s => statsMap[s.event] = parseInt(s.unique_visitors));
            
            const visitors = statsMap['page_view_landing'] || 0;
            const checkout = statsMap['checkout_clicked'] || 0;
            const conversionRate = visitors > 0 ? ((checkout / visitors) * 100).toFixed(1) : '0.0';
            
            return { visitors, checkout, conversionRate };
        }
        
        function renderFunnelStepsForLanguage(stats, containerId, color) {
            const mainFunnelOrder = [
                { key: 'page_view_landing', label: 'LANDING' },
                { key: 'page_view_phone', label: 'PHONE' },
                { key: 'phone_submitted', label: 'TEL ENVIADO' },
                { key: 'email_captured', label: 'EMAIL' },
                { key: 'page_view_conversas', label: 'CONVERSAS' },
                { key: 'page_view_cta', label: 'CTA' },
                { key: 'checkout_clicked', label: 'CHECKOUT' }
            ];
            
            const statsMap = {};
            (stats || []).forEach(s => statsMap[s.event] = parseInt(s.unique_visitors));
            
            // Enforce monotonic decrease: each step can't exceed the previous one
            let cappedValues = [];
            let prevCapped = Infinity;
            mainFunnelOrder.forEach(step => {
                const rawValue = statsMap[step.key] || 0;
                const capped = Math.min(rawValue, prevCapped);
                cappedValues.push({ ...step, value: capped, raw: rawValue });
                prevCapped = capped;
            });
            
            const maxVisitors = cappedValues.length > 0 ? Math.max(cappedValues[0].value, 1) : 1;
            
            let html = '';
            let prevValue = null;
            
            cappedValues.forEach(step => {
                const value = step.value;
                const width = maxVisitors > 0 ? Math.max((value / maxVisitors) * 100, 15) : 15;
                const drop = prevValue !== null && prevValue > 0 ? (((prevValue - value) / prevValue) * 100).toFixed(0) : null;
                
                html += `
                    <div class="funnel-step" style="margin-bottom: 6px; position: relative;">
                        <div class="funnel-bar" style="width: ${width}%; background: linear-gradient(90deg, ${color}, ${color}88); position: relative;">
                            <span class="funnel-step-name">${step.label}</span>
                            <span class="funnel-step-value">${value}</span>
                            ${drop > 0 ? `<span class="funnel-drop" style="position: absolute; right: -45px; top: 50%; transform: translateY(-50%); color: #ef4444; font-size: 12px; font-weight: 600;">-${drop}%</span>` : ''}
                        </div>
                    </div>
                `;
                prevValue = value;
            });
            
            const container = document.getElementById(containerId);
            if (container) {
                container.innerHTML = html || '<div class="empty-state"><p>Sem dados</p></div>';
            }
        }
        
        function updateFunnelHeader(lang) {
            // Legacy function - kept for compatibility
        }
        
        function updateRecoveryHeader(lang) {
            const indicator = document.getElementById('recoveryLangIndicator');
            if (!indicator) return;
            if (lang === 'en') {
                indicator.innerHTML = '<span class="lang-badge en">🇺🇸 English</span>';
            } else if (lang === 'es') {
                indicator.innerHTML = '<span class="lang-badge es">🇪🇸 Español</span>';
            } else {
                indicator.innerHTML = '<span class="lang-badge all">🌍 Todos</span>';
            }
        }
        
        function renderFunnelStats(stats) {
            // Main funnel steps (ordem corrigida: EMAIL capturado mais cedo, depois CONVERSAS e CTA)
            const mainFunnelOrder = [
                { key: 'page_view_landing', label: 'LANDING', section: 'main' },
                { key: 'page_view_phone', label: 'PHONE', section: 'main' },
                { key: 'phone_submitted', label: 'TEL ENVIADO', section: 'main' },
                { key: 'email_captured', label: 'EMAIL', section: 'main' },
                { key: 'page_view_conversas', label: 'CONVERSAS', section: 'main' },
                { key: 'page_view_cta', label: 'CTA', section: 'main' },
                { key: 'checkout_clicked', label: 'CHECKOUT', section: 'main' }
            ];
            
            // Upsell funnel steps
            const upsellFunnelOrder = [
                { key: 'upsell_1_view', label: 'Upsell 1 - View', section: 'upsell' },
                { key: 'upsell_1_accepted', label: 'Upsell 1 - Aceito ✅', section: 'upsell', isConversion: true },
                { key: 'upsell_1_declined', label: 'Upsell 1 - Recusado', section: 'upsell', isDecline: true },
                { key: 'upsell_2_view', label: 'Upsell 2 - View', section: 'upsell' },
                { key: 'upsell_2_accepted', label: 'Upsell 2 - Aceito ✅', section: 'upsell', isConversion: true },
                { key: 'upsell_2_declined', label: 'Upsell 2 - Recusado', section: 'upsell', isDecline: true },
                { key: 'upsell_3_view', label: 'Upsell 3 - View', section: 'upsell' },
                { key: 'upsell_3_accepted', label: 'Upsell 3 - Aceito ✅', section: 'upsell', isConversion: true },
                { key: 'upsell_3_declined', label: 'Upsell 3 - Recusado', section: 'upsell', isDecline: true },
                { key: 'thankyou_view', label: 'Obrigado', section: 'upsell' }
            ];
            
            const statsMap = {};
            (stats || []).forEach(s => statsMap[s.event] = parseInt(s.unique_visitors));
            
            const maxVisitors = Math.max(...Object.values(statsMap), 1);
            const landingVisitors = statsMap['page_view_landing'] || 0;
            const phoneVisitors = statsMap['page_view_phone'] || 0;
            const ctaVisitors = statsMap['page_view_cta'] || 0;
            const checkoutVisitors = statsMap['checkout_clicked'] || 0;
            const upsell1View = statsMap['upsell_1_view'] || 0;
            const upsell1Accept = statsMap['upsell_1_accepted'] || 0;
            const thankyouView = statsMap['thankyou_view'] || 0;
            
            document.getElementById('funnelVisitors').textContent = landingVisitors.toLocaleString('pt-BR');
            document.getElementById('funnelRate1').textContent = landingVisitors > 0 ? ((phoneVisitors / landingVisitors) * 100).toFixed(1) + '%' : '-';
            document.getElementById('funnelRate2').textContent = phoneVisitors > 0 ? ((ctaVisitors / phoneVisitors) * 100).toFixed(1) + '%' : '-';
            document.getElementById('funnelRate3').textContent = ctaVisitors > 0 ? ((checkoutVisitors / ctaVisitors) * 100).toFixed(1) + '%' : '-';
            
            // Enforce monotonic decrease for main funnel
            let cappedMain = [];
            let prevCap = Infinity;
            mainFunnelOrder.forEach(step => {
                const rawValue = statsMap[step.key] || 0;
                const capped = Math.min(rawValue, prevCap);
                cappedMain.push({ ...step, value: capped });
                prevCap = capped;
            });
            
            const cappedMax = cappedMain.length > 0 ? Math.max(cappedMain[0].value, 1) : 1;
            
            // Render main funnel
            let html = '<h4 style="color: var(--text-secondary); margin-bottom: 12px; font-size: 13px;">📊 FUNIL PRINCIPAL</h4>';
            let prevValue = null;
            cappedMain.forEach(step => {
                const value = step.value;
                const width = cappedMax > 0 ? Math.max((value / cappedMax) * 100, 15) : 15;
                const drop = prevValue !== null && prevValue > 0 ? (((prevValue - value) / prevValue) * 100).toFixed(0) : null;
                html += `<div class="funnel-step" style="position: relative; margin-bottom: 6px;"><div class="funnel-bar" style="width: ${width}%; position: relative;"><span class="funnel-step-name">${step.label}</span><span class="funnel-step-value">${value}</span>${drop > 0 ? `<span class="funnel-drop" style="position: absolute; right: -45px; top: 50%; transform: translateY(-50%); color: #ef4444; font-size: 12px; font-weight: 600;">-${drop}%</span>` : ''}</div></div>`;
                prevValue = value;
            });
            
            // Render upsell funnel if there's data
            const hasUpsellData = upsellFunnelOrder.some(step => statsMap[step.key] > 0);
            if (hasUpsellData) {
                html += '<h4 style="color: var(--text-secondary); margin: 24px 0 12px 0; font-size: 13px; border-top: 1px solid var(--border); padding-top: 20px;">🚀 UPSELLS</h4>';
                
                // Calculate upsell stats
                const up1Conv = upsell1View > 0 ? ((upsell1Accept / upsell1View) * 100).toFixed(1) : 0;
                const up2View = statsMap['upsell_2_view'] || 0;
                const up2Accept = statsMap['upsell_2_accepted'] || 0;
                const up2Conv = up2View > 0 ? ((up2Accept / up2View) * 100).toFixed(1) : 0;
                const up3View = statsMap['upsell_3_view'] || 0;
                const up3Accept = statsMap['upsell_3_accepted'] || 0;
                const up3Conv = up3View > 0 ? ((up3Accept / up3View) * 100).toFixed(1) : 0;
                
                html += `<div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin-bottom: 16px;">
                    <div style="background: var(--bg-tertiary); padding: 12px; border-radius: 8px; text-align: center;">
                        <div style="font-size: 20px; font-weight: 600; color: var(--accent);">${up1Conv}%</div>
                        <div style="font-size: 11px; color: var(--text-muted);">Upsell 1</div>
                    </div>
                    <div style="background: var(--bg-tertiary); padding: 12px; border-radius: 8px; text-align: center;">
                        <div style="font-size: 20px; font-weight: 600; color: var(--accent);">${up2Conv}%</div>
                        <div style="font-size: 11px; color: var(--text-muted);">Upsell 2</div>
                    </div>
                    <div style="background: var(--bg-tertiary); padding: 12px; border-radius: 8px; text-align: center;">
                        <div style="font-size: 20px; font-weight: 600; color: var(--accent);">${up3Conv}%</div>
                        <div style="font-size: 11px; color: var(--text-muted);">Upsell 3</div>
                    </div>
                </div>`;
                
                prevValue = checkoutVisitors; // Start from checkout
                upsellFunnelOrder.forEach(step => {
                    const value = statsMap[step.key] || 0;
                    if (value === 0 && !step.key.includes('view')) return; // Skip empty accept/decline
                    const width = maxVisitors > 0 ? Math.max((value / maxVisitors) * 100, 15) : 15;
                    let barColor = '';
                    if (step.isConversion) barColor = 'background: linear-gradient(90deg, var(--success), #16a34a);';
                    if (step.isDecline) barColor = 'background: linear-gradient(90deg, var(--danger), #dc2626);';
                    html += `<div class="funnel-step"><div class="funnel-bar" style="width: ${width}%; ${barColor}"><span class="funnel-step-name">${step.label}</span><span class="funnel-step-value">${value}</span></div></div>`;
                });
            }
            
            document.getElementById('funnelSteps').innerHTML = html || '<div class="empty-state"><h3>Sem dados</h3></div>';
        }
        
        function renderDailyStats(stats) {
            if (!stats?.length) { document.getElementById('dailyStats').innerHTML = '<div class="empty-state"><h3>Sem dados</h3></div>'; return; }
            const byDate = {};
            stats.forEach(s => { const date = s.date.split('T')[0]; if (!byDate[date]) byDate[date] = {}; byDate[date][s.event] = parseInt(s.unique_visitors); });
            let html = '<table class="data-table"><thead><tr><th>Data</th><th>Visitas</th><th>Checkout</th></tr></thead><tbody>';
            Object.keys(byDate).sort().reverse().slice(0, 7).forEach(date => {
                const visits = byDate[date]['page_view_landing'] || 0;
                const checkout = byDate[date]['checkout_clicked'] || 0;
                html += `<tr><td>${new Date(date + 'T12:00').toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit' })}</td><td>${visits}</td><td style="color:var(--accent-light)">${checkout}</td></tr>`;
            });
            html += '</tbody></table>';
            document.getElementById('dailyStats').innerHTML = html;
        }
        
        const eventLabelsGlobal = { 
            'page_view_landing': 'Landing', 
            'gender_selected': 'Gênero Selecionado', 
            'page_view_phone': 'Página Phone', 
            'phone_submitted': 'Telefone Enviado', 
            'page_view_conversas': 'Página Conversas', 
            'page_view_cta': 'Página CTA', 
            'email_captured': 'Email Capturado', 
            'checkout_clicked': 'Checkout Clicado',
            'checkout_50off_clicked': 'Checkout 50% OFF',
            'upsell_1_view': 'Upsell 1 - Visualizou',
            'upsell_1_accepted': 'Upsell 1 - Aceitou ✅',
            'upsell_1_declined': 'Upsell 1 - Recusou ❌',
            'upsell_2_view': 'Upsell 2 - Visualizou',
            'upsell_2_accepted': 'Upsell 2 - Aceitou ✅',
            'upsell_2_declined': 'Upsell 2 - Recusou ❌',
            'upsell_3_view': 'Upsell 3 - Visualizou',
            'upsell_3_accepted': 'Upsell 3 - Aceitou ✅',
            'upsell_3_declined': 'Upsell 3 - Recusou ❌',
            'thankyou_view': 'Página Obrigado'
        };
        
        function renderJourneys(journeys, showLanguage = false) {
            const colspan = showLanguage ? 8 : 7;
            if (!journeys?.length) { 
                document.getElementById('journeysTable').innerHTML = `<tr><td colspan="${colspan}"><div class="empty-state"><h3>Nenhuma jornada</h3></div></td></tr>`; 
                return; 
            }
            const shortLabels = { 
                'page_view_landing': 'Landing', 'gender_selected': 'Gênero', 'page_view_phone': 'Phone', 
                'phone_submitted': 'Tel.', 'page_view_conversas': 'Conversas', 'page_view_cta': 'CTA', 
                'email_captured': 'Email', 'checkout_clicked': 'Checkout', 'checkout_50off_clicked': '50%OFF',
                'upsell_1_view': 'Up1 👁️', 'upsell_1_accepted': 'Up1 ✅', 'upsell_1_declined': 'Up1 ❌',
                'upsell_2_view': 'Up2 👁️', 'upsell_2_accepted': 'Up2 ✅', 'upsell_2_declined': 'Up2 ❌',
                'upsell_3_view': 'Up3 👁️', 'upsell_3_accepted': 'Up3 ✅', 'upsell_3_declined': 'Up3 ❌',
                'thankyou_view': 'Obrigado'
            };
            let html = '';
            journeys.forEach(j => {
                const events = j.events || [];
                const langBadge = j.language === 'en' 
                    ? '<span style="background: rgba(59, 130, 246, 0.15); color: #3b82f6; padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: 600;">🇺🇸 EN</span>'
                    : j.language === 'es'
                    ? '<span style="background: rgba(245, 158, 11, 0.15); color: #f59e0b; padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: 600;">🇪🇸 ES</span>'
                    : '<span style="background: rgba(107, 114, 128, 0.15); color: #6b7280; padding: 2px 8px; border-radius: 12px; font-size: 11px;">-</span>';
                
                html += `<tr>
                    <td class="mono">${j.visitor_id.substring(0, 12)}...</td>
                    ${showLanguage ? `<td>${langBadge}</td>` : ''}
                    <td class="mono">${j.target_phone || '-'}</td>
                    <td>${j.target_gender === 'male' ? '👨' : j.target_gender === 'female' ? '👩' : '-'}</td>
                    <td><div class="journey-events">${events.slice(0, 5).map(e => `<span class="journey-tag ${e === 'checkout_clicked' ? 'checkout' : 'completed'}">${shortLabels[e] || e}</span>`).join('')}${events.length > 5 ? `<span class="journey-tag" style="cursor:pointer" onclick="openJourneyModal('${j.visitor_id}')">+${events.length - 5}</span>` : ''}</div></td>
                    <td>${formatDate(j.first_seen)}</td>
                    <td>${formatDate(j.last_seen)}</td>
                    <td><button class="action-btn" onclick="openJourneyModal('${j.visitor_id}')" title="Ver detalhes">👁️</button></td>
                </tr>`;
            });
            document.getElementById('journeysTable').innerHTML = html;
        }
        
        async function openJourneyModal(visitorId) {
            document.getElementById('journeyModal').classList.add('active');
            document.getElementById('journeyVisitorId').textContent = `ID: ${visitorId}`;
            document.getElementById('journeyLeadData').innerHTML = '<p style="color: var(--text-muted);">Carregando...</p>';
            document.getElementById('journeyEventsTimeline').innerHTML = '<p style="color: var(--text-muted);">Carregando...</p>';
            document.getElementById('journeyTransactionInfo').style.display = 'none';
            
            try {
                const response = await fetch(`${API_URL}/api/admin/funnel/visitor/${visitorId}`, {
                    headers: { 'Authorization': `Bearer ${authToken}` }
                });
                if (response.status === 401 || response.status === 403) { logout(); return; }
                const data = await response.json();
                
                // Render Lead Info
                if (data.lead) {
                    document.getElementById('journeyLeadData').innerHTML = `
                        <div><span style="color: var(--text-muted); font-size: 12px;">Nome</span><br><strong>${data.lead.name || '-'}</strong></div>
                        <div><span style="color: var(--text-muted); font-size: 12px;">Email</span><br><strong>${data.lead.email || '-'}</strong></div>
                        <div><span style="color: var(--text-muted); font-size: 12px;">WhatsApp</span><br><strong>${data.lead.whatsapp || '-'}</strong></div>
                        <div><span style="color: var(--text-muted); font-size: 12px;">Alvo</span><br><strong>${data.lead.target_phone || '-'}</strong></div>
                        <div><span style="color: var(--text-muted); font-size: 12px;">Gênero Alvo</span><br><strong>${data.lead.target_gender === 'male' ? '👨 Masculino' : data.lead.target_gender === 'female' ? '👩 Feminino' : '-'}</strong></div>
                        <div><span style="color: var(--text-muted); font-size: 12px;">Status</span><br><span class="badge badge-${data.lead.status || 'new'}"><span class="badge-dot"></span>${statusLabels[data.lead.status] || 'Novo'}</span></div>
                    `;
                } else {
                    document.getElementById('journeyLeadData').innerHTML = '<p style="color: var(--text-muted);">Lead não encontrado (visitante não preencheu o formulário)</p>';
                }
                
                // Render Transaction Info
                if (data.transaction) {
                    document.getElementById('journeyTransactionInfo').style.display = 'block';
                    const transStatus = data.transaction.status === 'approved' ? '✅ Aprovada' : data.transaction.status === 'refunded' ? '❌ Reembolsada' : data.transaction.status;
                    document.getElementById('journeyTransactionData').innerHTML = `
                        <div style="display: flex; gap: 24px;">
                            <div><span style="color: var(--text-muted); font-size: 12px;">Produto</span><br><strong>${data.transaction.product || '-'}</strong></div>
                            <div><span style="color: var(--text-muted); font-size: 12px;">Valor</span><br><strong style="color: var(--accent);">R$ ${data.transaction.value || '0'}</strong></div>
                            <div><span style="color: var(--text-muted); font-size: 12px;">Status</span><br><strong>${transStatus}</strong></div>
                        </div>
                    `;
                }
                
                // Render Events Timeline
                if (data.events?.length) {
                    let timelineHtml = '<div style="display: flex; flex-direction: column; gap: 8px;">';
                    data.events.forEach((event, index) => {
                        const isCheckout = event.event.includes('checkout');
                        const isAccepted = event.event.includes('accepted');
                        const isDeclined = event.event.includes('declined');
                        let dotColor = 'var(--accent)';
                        if (isCheckout) dotColor = 'var(--success)';
                        if (isAccepted) dotColor = 'var(--success)';
                        if (isDeclined) dotColor = 'var(--danger)';
                        
                        timelineHtml += `
                            <div style="display: flex; align-items: center; gap: 12px; padding: 8px 12px; background: var(--bg-secondary); border-radius: 8px;">
                                <div style="width: 10px; height: 10px; border-radius: 50%; background: ${dotColor}; flex-shrink: 0;"></div>
                                <div style="flex: 1;">
                                    <div style="font-weight: 500; font-size: 13px;">${eventLabelsGlobal[event.event] || event.event}</div>
                                    <div style="font-size: 11px; color: var(--text-muted);">${event.page || '-'}</div>
                                </div>
                                <div style="font-size: 11px; color: var(--text-muted); font-family: 'JetBrains Mono', monospace;">
                                    ${new Date(event.created_at).toLocaleString('pt-BR')}
                                </div>
                            </div>
                        `;
                    });
                    timelineHtml += '</div>';
                    document.getElementById('journeyEventsTimeline').innerHTML = timelineHtml;
                } else {
                    document.getElementById('journeyEventsTimeline').innerHTML = '<p style="color: var(--text-muted);">Nenhum evento encontrado</p>';
                }
                
            } catch (error) {
                console.error('Error loading journey:', error);
                document.getElementById('journeyEventsTimeline').innerHTML = '<p style="color: var(--danger);">Erro ao carregar jornada</p>';
            }
        }
        
        function closeJourneyModal() { document.getElementById('journeyModal').classList.remove('active'); }
        
        // Customer Journey Modal Functions
        async function openCustomerJourney(leadId) {
            document.getElementById('customerJourneyModal').classList.add('active');
            document.getElementById('customerTimeline').innerHTML = '<p style="color: var(--text-muted);">Carregando jornada...</p>';
            
            try {
                const response = await fetch(`${API_URL}/api/admin/customer/${leadId}/journey`, {
                    headers: { 'Authorization': `Bearer ${authToken}` }
                });
                
                if (!response.ok) throw new Error('Failed to load journey');
                
                const data = await response.json();
                renderCustomerJourney(data);
                
            } catch (error) {
                console.error('Error loading customer journey:', error);
                document.getElementById('customerTimeline').innerHTML = '<p style="color: var(--danger);">Erro ao carregar jornada</p>';
            }
        }
        
        function renderCustomerJourney(data) {
            const { lead, timeline, transactions, summary } = data;
            
            // Update subtitle
            const countryFlag = lead.country_code ? getCountryFlag(lead.country_code) : '🌍';
            document.getElementById('customerJourneySubtitle').textContent = `${lead.name || lead.email} • ${countryFlag} ${lead.country || 'País desconhecido'}`;
            
            // Update summary cards
            document.getElementById('summaryVisits').textContent = summary.visitCount || 1;
            document.getElementById('summaryEvents').textContent = summary.totalEvents || 0;
            document.getElementById('summaryPurchases').textContent = transactions.filter(t => t.status === 'approved').length;
            document.getElementById('summarySpent').textContent = `R$ ${parseFloat(summary.totalSpent || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}`;
            
            // Update customer info with better styling
            const statusBadge = lead.status === 'customer' 
                ? '<span style="background: linear-gradient(135deg, #10b981 0%, #059669 100%); color: white; padding: 4px 12px; border-radius: 20px; font-size: 11px; font-weight: 600;">Cliente</span>'
                : lead.status === 'contacted'
                ? '<span style="background: linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%); color: white; padding: 4px 12px; border-radius: 20px; font-size: 11px; font-weight: 600;">Contactado</span>'
                : '<span style="background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%); color: white; padding: 4px 12px; border-radius: 20px; font-size: 11px; font-weight: 600;">Novo</span>';
            
            const funnelBadge = lead.funnel_language === 'es' 
                ? '<span style="background: rgba(239, 68, 68, 0.15); color: #ef4444; padding: 4px 10px; border-radius: 6px; font-size: 11px;">🇪🇸 Espanhol</span>'
                : '<span style="background: rgba(59, 130, 246, 0.15); color: #3b82f6; padding: 4px 10px; border-radius: 6px; font-size: 11px;">🇺🇸 Inglês</span>';
            
            document.getElementById('customerInfoData').innerHTML = `
                <div style="display: flex; justify-content: space-between; align-items: center; padding: 8px 0; border-bottom: 1px solid rgba(255,255,255,0.05);">
                    <span style="color: rgba(255,255,255,0.5); font-size: 13px;">Nome</span>
                    <span style="color: #fff; font-weight: 500;">${lead.name || '-'}</span>
                </div>
                <div style="display: flex; justify-content: space-between; align-items: center; padding: 8px 0; border-bottom: 1px solid rgba(255,255,255,0.05);">
                    <span style="color: rgba(255,255,255,0.5); font-size: 13px;">Email</span>
                    <span style="color: #fff; font-weight: 500; font-size: 12px;">${lead.email}</span>
                </div>
                <div style="display: flex; justify-content: space-between; align-items: center; padding: 8px 0; border-bottom: 1px solid rgba(255,255,255,0.05);">
                    <span style="color: rgba(255,255,255,0.5); font-size: 13px;">WhatsApp</span>
                    <span style="color: #10b981; font-weight: 500; font-family: monospace;">${lead.whatsapp}</span>
                </div>
                <div style="display: flex; justify-content: space-between; align-items: center; padding: 8px 0; border-bottom: 1px solid rgba(255,255,255,0.05);">
                    <span style="color: rgba(255,255,255,0.5); font-size: 13px;">País</span>
                    <span style="color: #fff;">${countryFlag} ${lead.country || '-'} ${lead.city ? `<span style="color: rgba(255,255,255,0.4);">(${lead.city})</span>` : ''}</span>
                </div>
                <div style="display: flex; justify-content: space-between; align-items: center; padding: 8px 0; border-bottom: 1px solid rgba(255,255,255,0.05);">
                    <span style="color: rgba(255,255,255,0.5); font-size: 13px;">Funil</span>
                    ${funnelBadge}
                </div>
                <div style="display: flex; justify-content: space-between; align-items: center; padding: 8px 0;">
                    <span style="color: rgba(255,255,255,0.5); font-size: 13px;">Status</span>
                    ${statusBadge}
                </div>
            `;
            
            // Update products purchased
            const productsSection = document.getElementById('productsSection');
            const productsList = document.getElementById('productsPurchasedList');
            
            if (summary.productsPurchased && summary.productsPurchased.length > 0) {
                productsSection.style.display = 'block';
                productsList.innerHTML = summary.productsPurchased.map(p => {
                    const [type, name] = p.split(':');
                    const typeLabels = { 'front': '🎯 Front', 'upsell1': '⬆️ Upsell 1', 'upsell2': '⬆️ Upsell 2', 'upsell3': '⬆️ Upsell 3' };
                    return `<span class="badge" style="background: rgba(0,230,118,0.15); color: #00e676; padding: 6px 12px;">${typeLabels[type] || type}: ${name || 'Produto'}</span>`;
                }).join('');
            } else {
                productsSection.style.display = transactions.length === 0 ? 'none' : 'block';
                productsList.innerHTML = '<span style="color: var(--text-muted);">Nenhuma compra aprovada</span>';
            }
            
            // Render timeline
            if (timeline.length === 0) {
                document.getElementById('customerTimeline').innerHTML = `
                    <div style="text-align: center; padding: 40px 20px; color: rgba(255,255,255,0.4);">
                        <div style="font-size: 40px; margin-bottom: 12px; opacity: 0.5;">📭</div>
                        <p style="margin: 0;">Nenhum evento registrado ainda</p>
                    </div>
                `;
                return;
            }
            
            let html = '<div style="position: relative; padding-left: 32px;">';
            html += '<div style="position: absolute; left: 11px; top: 12px; bottom: 12px; width: 2px; background: linear-gradient(180deg, #6366f1 0%, #10b981 50%, #3b82f6 100%); border-radius: 2px;"></div>';
            
            timeline.forEach((item, index) => {
                const isFirst = index === 0;
                const date = new Date(item.timestamp);
                const timeStr = date.toLocaleString('pt-BR', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' });
                
                let bgColor = '#3b82f6';
                let icon = '📍';
                let glowColor = 'rgba(59, 130, 246, 0.3)';
                
                if (item.type === 'lead_captured') {
                    bgColor = '#6366f1';
                    glowColor = 'rgba(99, 102, 241, 0.4)';
                    icon = '🎉';
                } else if (item.type === 'transaction') {
                    if (item.event === 'approved') {
                        bgColor = '#10b981';
                        glowColor = 'rgba(16, 185, 129, 0.4)';
                        icon = '✅';
                    } else if (item.event === 'refunded' || item.event === 'chargeback') {
                        bgColor = '#ef4444';
                        glowColor = 'rgba(239, 68, 68, 0.4)';
                        icon = '💸';
                    } else {
                        bgColor = '#f59e0b';
                        glowColor = 'rgba(245, 158, 11, 0.4)';
                        icon = '⏳';
                    }
                }
                
                html += `
                    <div style="position: relative; padding-bottom: 12px;">
                        <div style="position: absolute; left: -26px; top: 4px; width: 24px; height: 24px; background: ${bgColor}; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 11px; box-shadow: 0 0 12px ${glowColor}; border: 2px solid rgba(255,255,255,0.1);">${icon}</div>
                        <div style="background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.06); border-radius: 12px; padding: 14px 16px; transition: all 0.2s; ${isFirst ? 'border-color: rgba(99, 102, 241, 0.3);' : ''}">
                            <div style="display: flex; justify-content: space-between; align-items: flex-start; gap: 12px;">
                                <div style="flex: 1;">
                                    <strong style="font-size: 13px; color: #fff; display: block; margin-bottom: 2px;">${item.label}</strong>
                                    ${item.details ? `<div style="font-size: 12px; color: rgba(255,255,255,0.5); margin-top: 4px;">
                                        ${item.type === 'transaction' ? `<span style="background: rgba(16, 185, 129, 0.15); color: #10b981; padding: 2px 8px; border-radius: 4px; font-size: 11px;">${item.details.productType}</span> <span style="color: #10b981; font-weight: 600;">R$ ${parseFloat(item.details.value || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</span>` : ''}
                                        ${item.page ? `<span style="color: rgba(255,255,255,0.4);">📄 ${item.page}</span>` : ''}
                                    </div>` : ''}
                                </div>
                                <span style="font-size: 11px; color: rgba(255,255,255,0.35); white-space: nowrap; padding: 4px 8px; background: rgba(255,255,255,0.03); border-radius: 6px;">${timeStr}</span>
                            </div>
                        </div>
                    </div>
                `;
            });
            
            html += '</div>';
            document.getElementById('customerTimeline').innerHTML = html;
        }
        
        function closeCustomerJourneyModal() { document.getElementById('customerJourneyModal').classList.remove('active'); }
        
        // Sales
        async function loadSalesData() {
            document.getElementById('postbackUrl').textContent = `${API_URL}/api/postback/monetizze`;
            try {
                const globalFilter = document.getElementById('globalFunnelFilter')?.value || '';
                const sourceFilter = getGlobalSourceFilter();
                const dateRange = getGlobalDateRange();
                
                // Build query params
                let params = [];
                if (globalFilter) params.push(`language=${globalFilter}`);
                if (sourceFilter) params.push(`source=${sourceFilter}`);
                if (dateRange) {
                    params.push(`startDate=${dateRange.startDate}`);
                    params.push(`endDate=${dateRange.endDate}`);
                }
                const queryString = params.length > 0 ? '?' + params.join('&') : '';
                
                const [statsRes, transRes] = await Promise.all([
                    fetch(`${API_URL}/api/admin/sales${queryString}`, { headers: { 'Authorization': `Bearer ${authToken}` } }),
                    fetch(`${API_URL}/api/admin/transactions${queryString}`, { headers: { 'Authorization': `Bearer ${authToken}` } })
                ]);
                if (statsRes.status === 401) { logout(); return; }
                const stats = await statsRes.json();
                const trans = await transRes.json();
                
                // Update header indicator
                updateSalesHeader(stats.language, stats.source);
                
                // Main stats
                const approved = stats.approved || 0;
                const revenue = stats.revenue || 0;
                const refunded = stats.refunded || 0;
                
                document.getElementById('salesApproved').textContent = approved.toLocaleString('pt-BR');
                document.getElementById('salesRevenue').textContent = 'R$ ' + revenue.toLocaleString('pt-BR', { minimumFractionDigits: 2 });
                document.getElementById('salesConversion').textContent = (stats.conversionRate || 0) + '%';
                document.getElementById('salesRefunded').textContent = refunded.toLocaleString('pt-BR');
                
                // Calculate unique customers from transactions
                const approvedTransactions = (trans.transactions || []).filter(t => t.status === 'approved');
                const uniqueCustomers = [...new Set(approvedTransactions.map(t => (t.email || '').toLowerCase()))].filter(e => e).length;
                
                // Ticket Médio (revenue / approved transactions)
                const avgTicket = approved > 0 ? (revenue / approved) : 0;
                document.getElementById('salesAvgTicket').textContent = 'R$ ' + avgTicket.toLocaleString('pt-BR', { minimumFractionDigits: 2 });
                
                // Vendas por Cliente (approved transactions / unique customers)
                const salesPerCustomer = uniqueCustomers > 0 ? (approved / uniqueCustomers).toFixed(1) : '0';
                document.getElementById('salesPerCustomer').textContent = salesPerCustomer;
                
                // Product stats are now shown only in the table
                
                // Product stats table
                renderProductStats(stats.byProduct);
                
                renderTransactions(trans.transactions);
            } catch (error) {
                console.error('Error loading sales:', error);
            }
        }
        
        function updateSalesHeader(lang, source) {
            const indicator = document.getElementById('salesLangIndicator');
            if (!indicator) return;
            let langText = '';
            if (lang === 'en') {
                langText = '<span class="lang-badge en">🇺🇸 English</span>';
            } else if (lang === 'es') {
                langText = '<span class="lang-badge es">🇪🇸 Spanish</span>';
            } else {
                langText = '<span class="lang-badge all">🌍 Todos</span>';
            }
            let sourceText = '';
            if (source === 'main') {
                sourceText = ' <span style="display:inline-flex;align-items:center;gap:4px;padding:4px 10px;border-radius:12px;font-size:12px;font-weight:600;background:rgba(59,130,246,0.15);color:#3b82f6;border:1px solid rgba(59,130,246,0.3);">🏠 Principal</span>';
            } else if (source === 'affiliate') {
                sourceText = ' <span style="display:inline-flex;align-items:center;gap:4px;padding:4px 10px;border-radius:12px;font-size:12px;font-weight:600;background:rgba(168,85,247,0.15);color:#a855f7;border:1px solid rgba(168,85,247,0.3);">🤝 Afiliados</span>';
            }
            indicator.innerHTML = langText + sourceText;
        }
        
        
        function renderProductStats(products) {
            if (!products?.length) {
                document.getElementById('productStatsTable').innerHTML = '<tr><td colspan="5"><div class="empty-state"><h3>Nenhum produto</h3></div></td></tr>';
                return;
            }
            let html = '';
            products.forEach(p => {
                const revenue = parseFloat(p.revenue) || 0;
                html += `<tr>
                    <td><strong>${p.product || 'Desconhecido'}</strong></td>
                    <td><span style="color: var(--success); font-weight: 600;">${p.approved || 0}</span></td>
                    <td><span style="color: var(--danger);">${p.refunded || 0}</span></td>
                    <td><span style="color: var(--accent-light); font-weight: 600;">R$ ${revenue.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</span></td>
                    <td>${p.total || 0}</td>
                </tr>`;
            });
            document.getElementById('productStatsTable').innerHTML = html;
        }
        
        function renderTransactions(transactions) {
            if (!transactions?.length) { document.getElementById('transactionsTable').innerHTML = '<tr><td colspan="9"><div class="empty-state"><div class="empty-state-icon">📦</div><h3>Nenhuma transação</h3></div></td></tr>'; return; }
            const statusLabelsT = { 'approved': 'Aprovada', 'pending_payment': 'Aguardando', 'cancelled': 'Cancelada', 'refunded': 'Reembolsada', 'chargeback': 'Chargeback' };
            let html = '';
            transactions.forEach(t => {
                const badgeClass = t.status === 'approved' ? 'badge-approved' : t.status === 'pending_payment' ? 'badge-pending' : 'badge-cancelled';
                const isAffiliate = t.funnel_source === 'affiliate';
                const sourceBadge = isAffiliate 
                    ? '<span style="display:inline-flex;align-items:center;gap:4px;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:600;background:rgba(168,85,247,0.15);color:#a855f7;border:1px solid rgba(168,85,247,0.3);">🤝 Afiliado</span>'
                    : '<span style="display:inline-flex;align-items:center;gap:4px;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:600;background:rgba(59,130,246,0.15);color:#3b82f6;border:1px solid rgba(59,130,246,0.3);">🏠 Principal</span>';
                const langBadge = t.funnel_language === 'es' ? '🇪🇸' : '🇺🇸';
                html += `<tr>
                    <td class="mono">${t.transaction_id?.substring(0, 12) || '-'}...</td>
                    <td>${t.name || '-'}</td>
                    <td>${t.email || '-'}</td>
                    <td>${t.product || '-'}</td>
                    <td style="color:var(--accent-light)">R$ ${t.value || '0'}</td>
                    <td>${langBadge} ${sourceBadge}</td>
                    <td><span class="badge ${badgeClass}"><span class="badge-dot"></span>${statusLabelsT[t.status] || t.status}</span></td>
                    <td>${formatDate(t.created_at)}</td>
                    <td><button class="action-btn danger" onclick="deleteTransaction(${t.id}, '${(t.product || '').replace(/'/g, "\\'")}' )" title="Deletar">🗑️</button></td>
                </tr>`;
            });
            document.getElementById('transactionsTable').innerHTML = html;
        }
        
        async function deleteTransaction(id, productName) {
            if (!confirm(`Tem certeza que deseja deletar a transação do produto "${productName}"?`)) return;
            
            try {
                const response = await fetch(`${API_URL}/api/admin/transactions/${id}`, {
                    method: 'DELETE',
                    headers: { 'Authorization': `Bearer ${authToken}` }
                });
                
                if (response.status === 401 || response.status === 403) { logout(); return; }
                
                if (response.ok) {
                    showToast('Deletado!', 'Transação removida com sucesso', 'success');
                    loadSalesData();
                } else {
                    showToast('Erro', 'Falha ao deletar transação', 'error');
                }
            } catch (error) {
                console.error('Error deleting transaction:', error);
                showToast('Erro', 'Falha ao deletar transação', 'error');
            }
        }
        
        // ==================== REFUNDS ====================
        
        let allRefunds = [];
        
        async function loadRefunds() {
            try {
                const dateRange = getGlobalDateRange();
                let url = `${API_URL}/api/admin/refunds`;
                
                if (dateRange) {
                    url += `?startDate=${dateRange.startDate}&endDate=${dateRange.endDate}`;
                }
                
                const response = await fetch(url, {
                    headers: { 'Authorization': `Bearer ${authToken}` }
                });
                
                if (response.status === 401 || response.status === 403) { logout(); return; }
                
                const data = await response.json();
                allRefunds = data.refunds || [];
                const stats = data.stats || {};
                
                // Update status stats
                const pending = allRefunds.filter(r => r.status === 'pending').length;
                const handling = allRefunds.filter(r => r.status === 'handling').length;
                const processing = allRefunds.filter(r => r.status === 'processing').length;
                const approved = allRefunds.filter(r => r.status === 'approved').length;
                const rejected = allRefunds.filter(r => r.status === 'rejected').length;
                
                document.getElementById('refundsPending').textContent = pending;
                document.getElementById('refundsHandling').textContent = handling;
                document.getElementById('refundsProcessing').textContent = processing;
                document.getElementById('refundsApproved').textContent = approved;
                document.getElementById('refundsRejected').textContent = rejected;
                document.getElementById('refundsTotalCount').textContent = `${allRefunds.length} solicitações`;
                document.getElementById('navRefundCount').textContent = pending + handling;
                
                // Update source stats
                const formStats = stats.form || { total: 0, pending: 0, approved: 0 };
                const monetizzeRefundStats = stats.monetizze_refund || { total: 0 };
                const chargebackStats = stats.monetizze_chargeback || { total: 0 };
                
                // Count from actual data for more accuracy
                const formTotal = allRefunds.filter(r => !r.source || r.source === 'form').length;
                const formPending = allRefunds.filter(r => (!r.source || r.source === 'form') && r.status === 'pending').length;
                const formApproved = allRefunds.filter(r => (!r.source || r.source === 'form') && r.status === 'approved').length;
                const monetizzeRefunds = allRefunds.filter(r => r.source === 'monetizze' && r.refund_type !== 'chargeback').length;
                const chargebacks = allRefunds.filter(r => r.source === 'monetizze' && r.refund_type === 'chargeback').length;
                
                // Count by language
                const formEN = allRefunds.filter(r => (!r.source || r.source === 'form') && r.funnel_language === 'en').length;
                const formES = allRefunds.filter(r => (!r.source || r.source === 'form') && r.funnel_language === 'es').length;
                const monetizzeEN = allRefunds.filter(r => r.source === 'monetizze' && r.refund_type !== 'chargeback' && r.funnel_language === 'en').length;
                const monetizzeES = allRefunds.filter(r => r.source === 'monetizze' && r.refund_type !== 'chargeback' && r.funnel_language === 'es').length;
                const chargebackEN = allRefunds.filter(r => r.source === 'monetizze' && r.refund_type === 'chargeback' && r.funnel_language === 'en').length;
                const chargebackES = allRefunds.filter(r => r.source === 'monetizze' && r.refund_type === 'chargeback' && r.funnel_language === 'es').length;
                
                const formTotalEl = document.getElementById('refundsFormTotal');
                const formPendingEl = document.getElementById('refundsFormPending');
                const formApprovedEl = document.getElementById('refundsFormApproved');
                const monetizzeTotalEl = document.getElementById('refundsMonetizzeTotal');
                const chargebackTotalEl = document.getElementById('refundsChargebackTotal');
                
                // Language elements
                const formENEl = document.getElementById('refundsFormEN');
                const formESEl = document.getElementById('refundsFormES');
                const monetizzeENEl = document.getElementById('refundsMonetizzeEN');
                const monetizzeESEl = document.getElementById('refundsMonetizzeES');
                const chargebackENEl = document.getElementById('refundsChargebackEN');
                const chargebackESEl = document.getElementById('refundsChargebackES');
                
                if (formTotalEl) formTotalEl.textContent = formTotal;
                if (formPendingEl) formPendingEl.textContent = formPending + ' pendentes';
                if (formApprovedEl) formApprovedEl.textContent = formApproved + ' aprovados';
                if (monetizzeTotalEl) monetizzeTotalEl.textContent = monetizzeRefunds;
                if (chargebackTotalEl) chargebackTotalEl.textContent = chargebacks;
                
                // Update language counters
                if (formENEl) formENEl.textContent = formEN;
                if (formESEl) formESEl.textContent = formES;
                if (monetizzeENEl) monetizzeENEl.textContent = monetizzeEN;
                if (monetizzeESEl) monetizzeESEl.textContent = monetizzeES;
                if (chargebackENEl) chargebackENEl.textContent = chargebackEN;
                if (chargebackESEl) chargebackESEl.textContent = chargebackES;
                
                renderRefunds(allRefunds);
                
            } catch (error) {
                console.error('Error loading refunds:', error);
            }
        }
        
        function filterRefunds() {
            const search = document.getElementById('refundSearchInput').value.toLowerCase();
            const status = document.getElementById('refundFilterStatus').value;
            const period = document.getElementById('refundFilterPeriod').value;
            const source = document.getElementById('refundFilterSource')?.value || '';
            const type = document.getElementById('refundFilterType')?.value || '';
            const language = document.getElementById('refundFilterLanguage')?.value || '';
            
            // Show/hide custom date fields
            const showCustomDates = period === 'custom';
            document.getElementById('customDateFilters').style.display = showCustomDates ? 'flex' : 'none';
            document.getElementById('customDateFiltersEnd').style.display = showCustomDates ? 'flex' : 'none';
            
            let filtered = allRefunds;
            
            // Filter by source
            if (source) {
                filtered = filtered.filter(r => (r.source || 'form') === source);
            }
            
            // Filter by type
            if (type) {
                filtered = filtered.filter(r => (r.refund_type || 'refund') === type);
            }
            
            // Filter by language
            if (language) {
                filtered = filtered.filter(r => r.funnel_language === language);
            }
            
            // Filter by status
            if (status) {
                filtered = filtered.filter(r => r.status === status);
            }
            
            // Filter by period
            if (period && period !== 'custom') {
                const now = new Date();
                const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
                let startDate, endDate = new Date();
                
                switch(period) {
                    case 'today':
                        startDate = today;
                        break;
                    case 'yesterday':
                        startDate = new Date(today);
                        startDate.setDate(startDate.getDate() - 1);
                        endDate = today;
                        break;
                    case 'week':
                        startDate = new Date(today);
                        startDate.setDate(startDate.getDate() - 7);
                        break;
                    case 'month':
                        startDate = new Date(today);
                        startDate.setDate(startDate.getDate() - 30);
                        break;
                }
                
                if (startDate) {
                    filtered = filtered.filter(r => {
                        if (!r.created_at) return false;
                        const refundDate = new Date(r.created_at);
                        return refundDate >= startDate && refundDate <= endDate;
                    });
                }
            }
            
            // Filter by custom date range
            if (period === 'custom') {
                const startDateVal = document.getElementById('refundDateStart').value;
                const endDateVal = document.getElementById('refundDateEnd').value;
                
                if (startDateVal) {
                    const startDate = new Date(startDateVal);
                    filtered = filtered.filter(r => {
                        if (!r.created_at) return false;
                        return new Date(r.created_at) >= startDate;
                    });
                }
                
                if (endDateVal) {
                    const endDate = new Date(endDateVal);
                    endDate.setHours(23, 59, 59, 999);
                    filtered = filtered.filter(r => {
                        if (!r.created_at) return false;
                        return new Date(r.created_at) <= endDate;
                    });
                }
            }
            
            // Filter by search
            if (search) {
                filtered = filtered.filter(r => 
                    (r.full_name || '').toLowerCase().includes(search) ||
                    (r.email || '').toLowerCase().includes(search) ||
                    (r.protocol || '').toLowerCase().includes(search) ||
                    (r.phone || '').toLowerCase().includes(search)
                );
            }
            
            renderRefunds(filtered);
        }
        
        function clearRefundFilters() {
            document.getElementById('refundFilterStatus').value = '';
            document.getElementById('refundFilterPeriod').value = '';
            document.getElementById('refundSearchInput').value = '';
            document.getElementById('refundDateStart').value = '';
            document.getElementById('refundDateEnd').value = '';
            const sourceFilter = document.getElementById('refundFilterSource');
            const typeFilter = document.getElementById('refundFilterType');
            if (sourceFilter) sourceFilter.value = '';
            if (typeFilter) typeFilter.value = '';
            document.getElementById('customDateFilters').style.display = 'none';
            document.getElementById('customDateFiltersEnd').style.display = 'none';
            renderRefunds(allRefunds);
        }
        
        // Theme Toggle
        function toggleTheme() {
            const html = document.documentElement;
            const currentTheme = html.getAttribute('data-theme');
            const newTheme = currentTheme === 'light' ? 'dark' : 'light';
            
            html.setAttribute('data-theme', newTheme);
            localStorage.setItem('adminTheme', newTheme);
            updateThemeIcon(newTheme);
        }
        
        function updateThemeIcon(theme) {
            const icon = document.getElementById('themeIcon');
            if (icon) {
                icon.textContent = theme === 'light' ? '☀️' : '🌙';
            }
        }
        
        function initTheme() {
            const savedTheme = localStorage.getItem('adminTheme') || 'dark';
            document.documentElement.setAttribute('data-theme', savedTheme);
            updateThemeIcon(savedTheme);
        }
        
        // Initialize theme on load
        initTheme();
        
        function renderRefunds(refunds) {
            const table = document.getElementById('refundsTable');
            
            if (!refunds || refunds.length === 0) {
                table.innerHTML = '<tr><td colspan="9" style="text-align: center; padding: 40px; color: var(--text-muted);">Nenhuma solicitação de reembolso encontrada</td></tr>';
                return;
            }
            
            const statusLabels = {
                'pending': { text: 'Pendente', class: 'status-pending' },
                'handling': { text: 'Tratando', class: 'status-handling' },
                'processing': { text: 'Processando', class: 'status-processing' },
                'approved': { text: 'Aprovado', class: 'status-approved' },
                'rejected': { text: 'Rejeitado', class: 'status-rejected' }
            };
            
            const reasonLabels = {
                'not_expected': 'Não atendeu expectativas',
                'technical': 'Problema técnico',
                'duplicate': 'Cobrança duplicada',
                'unauthorized': 'Não autorizado',
                'changed_mind': 'Desistiu da compra',
                'no_access': 'Sem acesso ao produto',
                'other': 'Outro motivo',
                'Reembolso via Monetizze': 'Reembolso Monetizze',
                'Chargeback - Disputa de cartão': 'Chargeback'
            };
            
            const sourceLabels = {
                'form': { text: '📝 Formulário', color: '#3b82f6' },
                'monetizze': { text: '💸 Monetizze', color: '#f59e0b' }
            };
            
            const typeLabels = {
                'refund': { text: '💸 Reembolso', color: '#f59e0b' },
                'chargeback': { text: '⚠️ Chargeback', color: '#ef4444' }
            };
            
            // Language labels
            const languageLabels = {
                'en': { text: '🇺🇸 EN', color: '#3b82f6' },
                'es': { text: '🇪🇸 ES', color: '#f59e0b' }
            };
            const defaultLang = { text: '🌍 N/A', color: '#71717a' };
            
            let html = '';
            refunds.forEach(r => {
                const status = statusLabels[r.status] || { text: r.status, class: 'status-pending' };
                const reason = reasonLabels[r.reason] || r.reason || '-';
                const date = r.created_at ? new Date(r.created_at).toLocaleDateString('pt-BR') : '-';
                const source = sourceLabels[r.source] || sourceLabels['form'];
                const refundType = r.source === 'monetizze' 
                    ? (typeLabels[r.refund_type] || typeLabels['refund'])
                    : null;
                const lang = languageLabels[r.funnel_language] || defaultLang;
                
                // Source/Type badge
                let sourceBadge = `<span style="display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 10px; font-weight: 600; background: ${source.color}22; color: ${source.color}; border: 1px solid ${source.color}44;">${source.text}</span>`;
                if (refundType && r.refund_type === 'chargeback') {
                    sourceBadge = `<span style="display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 10px; font-weight: 600; background: ${refundType.color}22; color: ${refundType.color}; border: 1px solid ${refundType.color}44;">${refundType.text}</span>`;
                }
                
                // Language badge
                const langBadge = `<span style="display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 10px; font-weight: 600; background: ${lang.color}22; color: ${lang.color}; border: 1px solid ${lang.color}44;">${lang.text}</span>`;
                
                // Value display
                const valueDisplay = r.value ? `R$ ${parseFloat(r.value).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}` : '-';
                
                html += `<tr>
                    <td><code class="protocol-code">${r.protocol || '-'}</code></td>
                    <td>${sourceBadge}</td>
                    <td>${langBadge}</td>
                    <td class="cell-name">${r.full_name || '-'}</td>
                    <td class="cell-email">${r.email || '-'}</td>
                    <td class="cell-reason" title="${reason}">${reason}</td>
                    <td style="font-weight: 600; color: var(--danger);">${valueDisplay}</td>
                    <td><span class="refund-status ${status.class}">${status.text}</span></td>
                    <td class="cell-date">${date}</td>
                    <td>
                        <div class="action-buttons" style="display: flex; gap: 4px; flex-wrap: wrap;">
                            <button class="action-btn-view" onclick="openRefundModal(${r.id})" title="Ver detalhes">👁️</button>
                            ${r.email ? `<button class="action-btn" onclick="quickEmail(${r.id})" title="Enviar Email" style="color: #3b82f6; cursor: pointer; background: none; border: none; font-size: 16px;">📧</button>` : ''}
                            ${(r.phone || r.whatsapp) ? `<button class="action-btn" onclick="quickWhatsApp(${r.id})" title="Enviar WhatsApp" style="color: #25d366; cursor: pointer; background: none; border: none; font-size: 16px;">💬</button>` : ''}
                            ${(r.status === 'pending' || r.status === 'handling' || r.status === 'processing') && r.source !== 'monetizze' ? `
                                <button class="action-btn-approve" onclick="updateRefundStatus(${r.id}, 'approved')" title="Aprovar">✓</button>
                                <button class="action-btn-reject" onclick="updateRefundStatus(${r.id}, 'rejected')" title="Rejeitar">✕</button>
                            ` : ''}
                        </div>
                    </td>
                </tr>`;
            });
            
            table.innerHTML = html;
        }
        
        // Quick communication from refund table
        function quickEmail(id) {
            const refund = allRefunds.find(r => r.id === id);
            if (!refund) return;
            const data = {
                name: refund.full_name || refund.name || 'Cliente',
                protocol: refund.protocol || 'N/A',
                email: refund.email,
                phone: refund.phone || refund.whatsapp,
                product: refund.product || 'Produto',
                reason: refund.reason || 'Não especificado'
            };
            sendEmailTo(data, 'followUp');
        }
        
        function quickWhatsApp(id) {
            const refund = allRefunds.find(r => r.id === id);
            if (!refund) return;
            const data = {
                name: refund.full_name || refund.name || 'Cliente',
                protocol: refund.protocol || 'N/A',
                email: refund.email,
                phone: refund.phone || refund.whatsapp,
                product: refund.product || 'Produto',
                reason: refund.reason || 'Não especificado'
            };
            sendWhatsAppTo(data, 'followUp');
        }
        
        async function openRefundModal(id) {
            const refund = allRefunds.find(r => r.id === id);
            if (!refund) return;
            
            const statusLabels = {
                'pending': { text: 'Pendente', class: 'status-pending' },
                'handling': { text: 'Tratando', class: 'status-handling' },
                'processing': { text: 'Processando', class: 'status-processing' },
                'approved': { text: 'Aprovado', class: 'status-approved' },
                'rejected': { text: 'Rejeitado', class: 'status-rejected' }
            };
            
            const reasonLabels = {
                'not_expected': 'Produto não atendeu expectativas',
                'technical': 'Problema técnico de acesso',
                'duplicate': 'Cobrança duplicada no cartão',
                'unauthorized': 'Compra não autorizada',
                'changed_mind': 'Desistiu da compra',
                'no_access': 'Não recebeu acesso ao produto',
                'other': 'Outro motivo'
            };
            
            const date = refund.created_at ? new Date(refund.created_at).toLocaleString('pt-BR') : '-';
            const purchaseDate = refund.purchase_date ? new Date(refund.purchase_date).toLocaleDateString('pt-BR') : '-';
            const statusInfo = statusLabels[refund.status] || { text: refund.status, class: 'status-pending' };
            
            // Language badge for modal
            const modalLangLabels = {
                'en': { text: '🇺🇸 Inglês', color: '#3b82f6' },
                'es': { text: '🇪🇸 Espanhol', color: '#f59e0b' }
            };
            const modalDefaultLang = { text: '🌍 Não definido', color: '#71717a' };
            const modalLang = modalLangLabels[refund.funnel_language] || modalDefaultLang;
            const modalLangBadge = `<span style="display: inline-block; padding: 4px 10px; border-radius: 12px; font-size: 11px; font-weight: 600; background: ${modalLang.color}22; color: ${modalLang.color}; border: 1px solid ${modalLang.color}44;">${modalLang.text}</span>`;
            
            // Source/Type badge for modal
            const modalSourceLabels = {
                'form': { text: '📝 Formulário', color: '#3b82f6' },
                'monetizze': { text: '💸 Monetizze', color: '#f59e0b' }
            };
            const modalTypeLabels = {
                'refund': { text: '💸 Monetizze', color: '#f59e0b' },
                'chargeback': { text: '⚠️ Chargeback', color: '#ef4444' }
            };
            const modalSource = modalSourceLabels[refund.source] || modalSourceLabels['form'];
            let modalSourceBadge = `<span style="display: inline-block; padding: 4px 10px; border-radius: 12px; font-size: 11px; font-weight: 600; background: ${modalSource.color}22; color: ${modalSource.color}; border: 1px solid ${modalSource.color}44;">${modalSource.text}</span>`;
            if (refund.source === 'monetizze' && refund.refund_type === 'chargeback') {
                const modalType = modalTypeLabels['chargeback'];
                modalSourceBadge = `<span style="display: inline-block; padding: 4px 10px; border-radius: 12px; font-size: 11px; font-weight: 600; background: ${modalType.color}22; color: ${modalType.color}; border: 1px solid ${modalType.color}44;">${modalType.text}</span>`;
            }
            
            // Show loading state in modal
            document.getElementById('refundModalBody').innerHTML = `
                <div style="text-align: center; padding: 40px;">
                    <div class="skeleton-line" style="width: 60%; height: 20px; margin: 0 auto 16px;"></div>
                    <div class="skeleton-line" style="width: 80%; height: 14px; margin: 0 auto 8px;"></div>
                    <div class="skeleton-line" style="width: 40%; height: 14px; margin: 0 auto;"></div>
                    <p style="color: var(--text-muted); margin-top: 16px;">Carregando dados cruzados...</p>
                </div>
            `;
            document.getElementById('refundModal').style.display = 'flex';
            
            // Fetch enriched data from API
            let crossRef = null;
            try {
                const response = await fetch(`/api/admin/refunds/${id}/details`, {
                    headers: { 'Authorization': `Bearer ${token}` }
                });
                if (response.status === 401 || response.status === 403) { logout(); return; }
                if (response.ok) {
                    const data = await response.json();
                    crossRef = data.crossReference || null;
                }
            } catch (err) {
                console.error('Error fetching cross-ref data:', err);
            }
            
            // Build cross-reference section
            let crossRefHTML = '';
            if (crossRef && crossRef.summary) {
                const s = crossRef.summary;
                const detLang = modalLangLabels[s.detectedLanguage];
                const detLangText = detLang ? detLang.text : '🌍 Não identificado';
                
                crossRefHTML = `
                    <div class="refund-section" style="margin-top: 16px;">
                        <h4 class="refund-section-title">🔗 DADOS CRUZADOS DO FUNIL</h4>
                        <div class="refund-info-box" style="background: linear-gradient(135deg, rgba(99, 102, 241, 0.05), rgba(139, 92, 246, 0.05)); border: 1px solid rgba(99, 102, 241, 0.15);">
                            <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; margin-bottom: 12px;">
                                <div style="padding: 12px; background: rgba(99, 102, 241, 0.08); border-radius: 10px; text-align: center;">
                                    <div style="font-size: 20px; font-weight: 700; color: #6366f1;">${s.totalTransactions}</div>
                                    <div style="font-size: 10px; color: var(--text-muted); font-weight: 600; text-transform: uppercase;">Transações</div>
                                </div>
                                <div style="padding: 12px; background: rgba(34, 197, 94, 0.08); border-radius: 10px; text-align: center;">
                                    <div style="font-size: 20px; font-weight: 700; color: #22c55e;">R$ ${s.totalSpent.toLocaleString('pt-BR', {minimumFractionDigits: 2})}</div>
                                    <div style="font-size: 10px; color: var(--text-muted); font-weight: 600; text-transform: uppercase;">Total Gasto</div>
                                </div>
                            </div>
                            
                            <div class="refund-info-row"><span class="refund-label">Idioma detectado:</span> <span class="refund-value">${detLangText}</span></div>
                            <div class="refund-info-row"><span class="refund-label">Vendas aprovadas:</span> <span class="refund-value">${s.approvedTransactions}</span></div>
                            <div class="refund-info-row"><span class="refund-label">Etapas no funil:</span> <span class="refund-value">${s.funnelSteps}</span></div>
                            ${s.productsBought.length > 0 ? `
                            <div class="refund-info-row"><span class="refund-label">Produtos comprados:</span> <span class="refund-value">${s.productsBought.join(', ')}</span></div>
                            ` : ''}
                        </div>
                    </div>
                    
                    ${crossRef.transactions && crossRef.transactions.length > 0 ? `
                    <div class="refund-section" style="margin-top: 12px;">
                        <h4 class="refund-section-title">💳 HISTÓRICO DE TRANSAÇÕES</h4>
                        <div style="max-height: 200px; overflow-y: auto;">
                            ${crossRef.transactions.map(tx => {
                                const txDate = tx.created_at ? new Date(tx.created_at).toLocaleDateString('pt-BR') : '-';
                                const txStatus = tx.status === 'approved' ? '✅' : tx.status === 'refunded' ? '🔄' : tx.status === 'chargeback' ? '⚠️' : '⏳';
                                const txValue = tx.value ? 'R$ ' + parseFloat(tx.value).toLocaleString('pt-BR', {minimumFractionDigits: 2}) : '-';
                                return `<div style="display: flex; justify-content: space-between; align-items: center; padding: 8px 12px; background: rgba(255,255,255,0.03); border-radius: 8px; margin-bottom: 4px; font-size: 12px;">
                                    <span style="color: var(--text-secondary);">${txStatus} ${tx.product || 'Produto'}</span>
                                    <span style="font-weight: 600; color: var(--text-primary);">${txValue}</span>
                                    <span style="color: var(--text-muted);">${txDate}</span>
                                </div>`;
                            }).join('')}
                        </div>
                    </div>
                    ` : ''}
                    
                    ${crossRef.lead ? `
                    <div class="refund-section" style="margin-top: 12px;">
                        <h4 class="refund-section-title">👤 DADOS DO LEAD</h4>
                        <div class="refund-info-box">
                            <div class="refund-info-row"><span class="refund-label">Status no funil:</span> <span class="refund-value">${crossRef.lead.status || '-'}</span></div>
                            <div class="refund-info-row"><span class="refund-label">País:</span> <span class="refund-value">${crossRef.lead.country || '-'}</span></div>
                            <div class="refund-info-row"><span class="refund-label">Origem:</span> <span class="refund-value">${crossRef.lead.source || '-'}</span></div>
                            <div class="refund-info-row"><span class="refund-label">Primeira compra:</span> <span class="refund-value">${crossRef.lead.first_purchase_at ? new Date(crossRef.lead.first_purchase_at).toLocaleDateString('pt-BR') : '-'}</span></div>
                            <div class="refund-info-row"><span class="refund-label">Última compra:</span> <span class="refund-value">${crossRef.lead.last_purchase_at ? new Date(crossRef.lead.last_purchase_at).toLocaleDateString('pt-BR') : '-'}</span></div>
                        </div>
                    </div>
                    ` : '<div style="padding: 12px; background: rgba(245, 158, 11, 0.08); border-radius: 8px; border: 1px solid rgba(245, 158, 11, 0.2); margin-top: 12px; font-size: 12px; color: #f59e0b;">⚠️ Lead não encontrado no funil - este cliente pode ter comprado fora do funil rastreado.</div>'}
                `;
            }
            
            document.getElementById('refundModalBody').innerHTML = `
                <div class="refund-modal-content">
                    <div class="refund-modal-header-info" style="display: flex; flex-wrap: wrap; gap: 8px; align-items: center;">
                        <code class="protocol-code">${refund.protocol || '-'}</code>
                        <span class="refund-status ${statusInfo.class}">${statusInfo.text}</span>
                        ${modalLangBadge}
                        ${modalSourceBadge}
                    </div>
                    
                    <div class="refund-section">
                        <h4 class="refund-section-title">👤 INFORMAÇÕES DO CLIENTE</h4>
                        <div class="refund-info-box">
                            <div class="refund-info-row"><span class="refund-label">Nome:</span> <span class="refund-value">${refund.full_name || '-'}</span></div>
                            <div class="refund-info-row"><span class="refund-label">Email:</span> <span class="refund-value">${refund.email || '-'}</span></div>
                            <div class="refund-info-row"><span class="refund-label">Telefone:</span> <span class="refund-value">${refund.phone || '-'}</span></div>
                        </div>
                    </div>
                    
                    <div class="refund-section">
                        <h4 class="refund-section-title">📦 DETALHES DA COMPRA</h4>
                        <div class="refund-info-box">
                            <div class="refund-info-row"><span class="refund-label">Data da Compra:</span> <span class="refund-value">${purchaseDate}</span></div>
                            <div class="refund-info-row"><span class="refund-label">Motivo:</span> <span class="refund-value refund-reason">${reasonLabels[refund.reason] || refund.reason || '-'}</span></div>
                            ${refund.value ? `<div class="refund-info-row"><span class="refund-label">Valor:</span> <span class="refund-value" style="color: var(--danger); font-weight: 600;">R$ ${parseFloat(refund.value).toLocaleString('pt-BR', {minimumFractionDigits: 2})}</span></div>` : ''}
                        </div>
                    </div>
                    
                    <div class="refund-section">
                        <h4 class="refund-section-title">💬 DESCRIÇÃO</h4>
                        <div class="refund-description-box">
                            ${refund.details || 'Nenhuma descrição fornecida.'}
                        </div>
                    </div>
                    
                    ${refund.admin_notes ? `
                    <div class="refund-section">
                        <h4 class="refund-section-title">📝 NOTAS DO ADMIN</h4>
                        <div class="refund-description-box">
                            ${refund.admin_notes}
                        </div>
                    </div>
                    ` : ''}
                    
                    ${crossRefHTML}
                    
                    <div class="refund-timestamp">
                        Solicitado em: ${date}
                    </div>
                    
                    <!-- Quick Communication -->
                    ${refund.email || refund.phone || refund.whatsapp ? `
                    <div style="margin-top: 20px; padding: 20px; background: linear-gradient(135deg, rgba(59, 130, 246, 0.08), rgba(37, 211, 102, 0.08)); border-radius: 12px; border: 1px solid rgba(59, 130, 246, 0.2);">
                        <h4 style="margin: 0 0 16px 0; font-size: 14px; font-weight: 600; color: var(--text-primary);">⚡ Comunicação Rápida</h4>
                        
                        ${refund.email ? `
                        <div style="margin-bottom: 12px;">
                            <div style="font-size: 11px; color: var(--text-muted); margin-bottom: 8px; font-weight: 600;">📧 EMAIL</div>
                            <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 8px;">
                                <button onclick="quickEmail(${refund.id})" style="padding: 10px; border-radius: 8px; border: 1px solid rgba(59, 130, 246, 0.3); background: rgba(59, 130, 246, 0.1); color: #3b82f6; cursor: pointer; font-size: 13px; font-weight: 500;">📝 Solicitar Info</button>
                                <button onclick="sendEmailTo({name:'${(refund.full_name||'').replace(/'/g,"\\'")}',protocol:'${refund.protocol||''}',email:'${refund.email||''}',product:'${(refund.product||'').replace(/'/g,"\\'")}',reason:'${(refund.reason||'').replace(/'/g,"\\'")}'},'approved')" style="padding: 10px; border-radius: 8px; border: 1px solid rgba(34, 197, 94, 0.3); background: rgba(34, 197, 94, 0.1); color: #22c55e; cursor: pointer; font-size: 13px; font-weight: 500;">✅ Aprovação</button>
                                <button onclick="sendEmailTo({name:'${(refund.full_name||'').replace(/'/g,"\\'")}',protocol:'${refund.protocol||''}',email:'${refund.email||''}',product:'${(refund.product||'').replace(/'/g,"\\'")}',reason:'${(refund.reason||'').replace(/'/g,"\\'")}'},'rejected')" style="padding: 10px; border-radius: 8px; border: 1px solid rgba(239, 68, 68, 0.3); background: rgba(239, 68, 68, 0.1); color: #ef4444; cursor: pointer; font-size: 13px; font-weight: 500;">❌ Rejeição</button>
                                <button onclick="sendEmailTo({name:'${(refund.full_name||'').replace(/'/g,"\\'")}',protocol:'${refund.protocol||''}',email:'${refund.email||''}',product:'${(refund.product||'').replace(/'/g,"\\'")}',reason:'${(refund.reason||'').replace(/'/g,"\\'")}'},'followUp')" style="padding: 10px; border-radius: 8px; border: 1px solid rgba(59, 130, 246, 0.3); background: rgba(59, 130, 246, 0.1); color: #3b82f6; cursor: pointer; font-size: 13px; font-weight: 500;">👋 Acompanhamento</button>
                            </div>
                        </div>
                        ` : ''}
                        
                        ${refund.phone || refund.whatsapp ? `
                        <div>
                            <div style="font-size: 11px; color: var(--text-muted); margin-bottom: 8px; font-weight: 600;">💬 WHATSAPP</div>
                            <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 8px;">
                                <button onclick="quickWhatsApp(${refund.id})" style="padding: 10px; border-radius: 8px; border: 1px solid rgba(37, 211, 102, 0.3); background: rgba(37, 211, 102, 0.1); color: #25d366; cursor: pointer; font-size: 13px; font-weight: 500;">📝 Solicitar Info</button>
                                <button onclick="sendWhatsAppTo({name:'${(refund.full_name||'').replace(/'/g,"\\'")}',protocol:'${refund.protocol||''}',email:'${refund.email||''}',phone:'${refund.phone||refund.whatsapp||''}',product:'${(refund.product||'').replace(/'/g,"\\'")}',reason:'${(refund.reason||'').replace(/'/g,"\\'")}'},'approved')" style="padding: 10px; border-radius: 8px; border: 1px solid rgba(37, 211, 102, 0.3); background: rgba(37, 211, 102, 0.1); color: #25d366; cursor: pointer; font-size: 13px; font-weight: 500;">✅ Aprovação</button>
                                <button onclick="sendWhatsAppTo({name:'${(refund.full_name||'').replace(/'/g,"\\'")}',protocol:'${refund.protocol||''}',email:'${refund.email||''}',phone:'${refund.phone||refund.whatsapp||''}',product:'${(refund.product||'').replace(/'/g,"\\'")}',reason:'${(refund.reason||'').replace(/'/g,"\\'")}'},'rejected')" style="padding: 10px; border-radius: 8px; border: 1px solid rgba(37, 211, 102, 0.3); background: rgba(37, 211, 102, 0.1); color: #25d366; cursor: pointer; font-size: 13px; font-weight: 500;">❌ Rejeição</button>
                                <button onclick="sendWhatsAppTo({name:'${(refund.full_name||'').replace(/'/g,"\\'")}',protocol:'${refund.protocol||''}',email:'${refund.email||''}',phone:'${refund.phone||refund.whatsapp||''}',product:'${(refund.product||'').replace(/'/g,"\\'")}',reason:'${(refund.reason||'').replace(/'/g,"\\'")}'},'followUp')" style="padding: 10px; border-radius: 8px; border: 1px solid rgba(37, 211, 102, 0.3); background: rgba(37, 211, 102, 0.1); color: #25d366; cursor: pointer; font-size: 13px; font-weight: 500;">👋 Acompanhamento</button>
                            </div>
                        </div>
                        ` : ''}
                    </div>
                    ` : ''}
                </div>
            `;
            
            // Actions footer - show buttons for pending, handling, or processing
            if (refund.status === 'pending' || refund.status === 'handling' || refund.status === 'processing') {
                document.getElementById('refundModalFooter').innerHTML = `
                    <div class="refund-modal-actions">
                        ${refund.status === 'pending' ? `
                            <button class="modal-btn modal-btn-handling" onclick="updateRefundStatus(${refund.id}, 'handling'); closeRefundModal();">
                                <span>🎯</span> Iniciar Tratativa
                            </button>
                        ` : ''}
                        ${refund.status === 'handling' ? `
                            <button class="modal-btn modal-btn-processing" onclick="updateRefundStatus(${refund.id}, 'processing'); closeRefundModal();">
                                <span>🔄</span> Marcar Processando
                            </button>
                        ` : ''}
                        <button class="modal-btn modal-btn-reject" onclick="updateRefundStatus(${refund.id}, 'rejected'); closeRefundModal();">
                            <span>✕</span> Rejeitar
                        </button>
                        <button class="modal-btn modal-btn-approve" onclick="updateRefundStatus(${refund.id}, 'approved'); closeRefundModal();">
                            <span>✓</span> Aprovar Reembolso
                        </button>
                    </div>
                `;
            } else {
                document.getElementById('refundModalFooter').innerHTML = `
                    <div style="display: flex; gap: 12px; justify-content: flex-end;">
                        <button class="btn-secondary" onclick="closeRefundModal();">Fechar</button>
                    </div>
                `;
            }
            
            document.getElementById('refundModal').style.display = 'flex';
        }
        
        function closeRefundModal() {
            document.getElementById('refundModal').style.display = 'none';
        }
        
        async function updateRefundStatus(id, status) {
            const notes = status === 'rejected' ? prompt('Motivo da rejeição (opcional):') : '';
            
            try {
                const response = await fetch(`${API_URL}/api/admin/refunds/${id}`, {
                    method: 'PUT',
                    headers: {
                        'Authorization': `Bearer ${authToken}`,
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({ status, notes })
                });
                
                if (response.status === 401 || response.status === 403) { logout(); return; }
                
                if (response.ok) {
                    const statusText = status === 'approved' ? 'aprovado' : status === 'rejected' ? 'rejeitado' : 'atualizado';
                    showToast('Sucesso!', `Reembolso ${statusText} com sucesso`, 'success');
                    loadRefunds();
                } else {
                    showToast('Erro', 'Falha ao atualizar reembolso', 'error');
                }
            } catch (error) {
                console.error('Error updating refund:', error);
                showToast('Erro', 'Falha ao atualizar reembolso', 'error');
            }
        }
        
        // ==================== COUNTRIES REPORT ====================
        async function loadCountriesData() {
            const container = document.getElementById('countriesListContainer');
            
            // Show loading state
            container.innerHTML = `
                <div style="padding: 60px 24px; text-align: center;">
                    <div class="spinner" style="margin: 0 auto 16px;"></div>
                    <p style="color: rgba(255,255,255,0.4);">Carregando dados geográficos...</p>
                </div>
            `;
            
            try {
                const headers = { 'Authorization': `Bearer ${authToken}` };
                const dateRange = getGlobalDateRange();
                
                // Build URLs with date filters
                let leadsUrl = `${API_URL}/api/admin/leads?limit=10000`;
                let transactionsUrl = `${API_URL}/api/admin/transactions`;
                
                if (dateRange) {
                    leadsUrl += `&startDate=${dateRange.startDate}&endDate=${dateRange.endDate}`;
                    transactionsUrl += `?startDate=${dateRange.startDate}&endDate=${dateRange.endDate}`;
                }
                
                // Fetch leads and transactions
                const [leadsRes, transactionsRes] = await Promise.all([
                    fetch(leadsUrl, { headers }),
                    fetch(transactionsUrl, { headers })
                ]);
                
                if (!leadsRes.ok || !transactionsRes.ok) {
                    throw new Error('Erro ao buscar dados');
                }
                
                const leadsData = await leadsRes.json();
                const transactionsData = await transactionsRes.json();
                
                const leads = leadsData.leads || [];
                const transactions = transactionsData.transactions || [];
                
                console.log('Countries - Leads:', leads.length, 'Transactions:', transactions.length);
                
                // Group by country
                const countryStats = {};
                
                // Process leads
                leads.forEach(lead => {
                    const country = lead.country || 'Desconhecido';
                    const countryCode = lead.country_code || 'XX';
                    const key = countryCode;
                    
                    if (!countryStats[key]) {
                        countryStats[key] = {
                            country: country,
                            country_code: countryCode,
                            leads: 0,
                            converted: 0,
                            revenue: 0,
                            emails: new Set()
                        };
                    }
                    
                    countryStats[key].leads++;
                    if (lead.email) {
                        countryStats[key].emails.add(lead.email.toLowerCase());
                    }
                    
                    if (lead.status === 'converted') {
                        countryStats[key].converted++;
                    }
                });
                
                // Process transactions to add revenue
                transactions.forEach(tx => {
                    if (tx.status !== 'approved') return;
                    
                    const email = (tx.email || '').toLowerCase();
                    const value = parseFloat(tx.value) || 0;
                    
                    // Find which country this transaction belongs to
                    for (const key in countryStats) {
                        if (countryStats[key].emails.has(email)) {
                            countryStats[key].revenue += value;
                            break;
                        }
                    }
                });
                
                // Convert to array and calculate metrics
                const countriesArray = Object.values(countryStats)
                    .map(c => ({
                        ...c,
                        conversionRate: c.leads > 0 ? ((c.converted / c.leads) * 100) : 0,
                        avgTicket: c.converted > 0 ? (c.revenue / c.converted) : 0
                    }))
                    .sort((a, b) => b.leads - a.leads);
                
                // Count real countries (exclude unknown)
                const realCountries = countriesArray.filter(c => c.country_code !== 'XX' && c.country !== 'Desconhecido');
                
                // Update summary cards
                document.getElementById('countriesTotal').textContent = realCountries.length;
                document.getElementById('countriesCount').textContent = `${countriesArray.length} países`;
                
                if (countriesArray.length > 0) {
                    // Top by leads
                    const top = countriesArray[0];
                    document.getElementById('topCountry').innerHTML = `${getCountryFlag(top.country_code)} ${top.country_code}`;
                    document.getElementById('topCountryLeads').textContent = `${top.leads} leads`;
                    
                    // Best conversion (min 2 leads to be fair)
                    const bestConversion = [...countriesArray]
                        .filter(c => c.leads >= 2 && c.converted > 0)
                        .sort((a, b) => b.conversionRate - a.conversionRate)[0];
                    
                    if (bestConversion) {
                        document.getElementById('bestConversionCountry').innerHTML = `${getCountryFlag(bestConversion.country_code)} ${bestConversion.country_code}`;
                        document.getElementById('bestConversionRate').textContent = `${bestConversion.conversionRate.toFixed(1)}%`;
                    } else {
                        document.getElementById('bestConversionCountry').textContent = '-';
                        document.getElementById('bestConversionRate').textContent = '0%';
                    }
                    
                    // Top revenue
                    const topRevenue = [...countriesArray].sort((a, b) => b.revenue - a.revenue)[0];
                    if (topRevenue && topRevenue.revenue > 0) {
                        document.getElementById('topRevenueCountry').innerHTML = `${getCountryFlag(topRevenue.country_code)} ${topRevenue.country_code}`;
                        document.getElementById('topRevenueValue').textContent = `$${topRevenue.revenue.toLocaleString('en-US', { minimumFractionDigits: 2 })}`;
                    } else {
                        document.getElementById('topRevenueCountry').textContent = '-';
                        document.getElementById('topRevenueValue').textContent = '$0';
                    }
                }
                
                // Render modern list
                renderCountriesList(countriesArray);
                
            } catch (error) {
                console.error('Error loading countries data:', error);
                container.innerHTML = `
                    <div style="padding: 60px 24px; text-align: center; color: rgba(255,255,255,0.4);">
                        <div style="font-size: 48px; margin-bottom: 16px;">❌</div>
                        <h3 style="margin: 0 0 8px; color: #ef4444;">Erro ao carregar dados</h3>
                        <p style="margin: 0;">${error.message || 'Tente novamente'}</p>
                    </div>
                `;
            }
        }
        
        function renderCountriesList(countries) {
            const container = document.getElementById('countriesListContainer');
            
            if (!countries || countries.length === 0) {
                container.innerHTML = `
                    <div style="padding: 60px 24px; text-align: center; color: rgba(255,255,255,0.4);">
                        <div style="font-size: 48px; margin-bottom: 16px; opacity: 0.5;">🌍</div>
                        <h3 style="margin: 0 0 8px; color: rgba(255,255,255,0.6);">Nenhum dado disponível</h3>
                        <p style="margin: 0;">Os leads ainda não possuem informação de localização.</p>
                    </div>
                `;
                return;
            }
            
            // Calculate totals for percentage bars
            const maxLeads = Math.max(...countries.map(c => c.leads));
            
            let html = '';
            countries.forEach((country, index) => {
                const leadsPercent = maxLeads > 0 ? (country.leads / maxLeads) * 100 : 0;
                const conversionColor = country.conversionRate >= 5 ? '#10b981' : country.conversionRate >= 2 ? '#f59e0b' : '#ef4444';
                const isTop3 = index < 3;
                const medalIcon = index === 0 ? '🥇' : index === 1 ? '🥈' : index === 2 ? '🥉' : '';
                
                html += `
                    <div style="padding: 16px 24px; border-bottom: 1px solid rgba(255,255,255,0.04); display: grid; grid-template-columns: 50px 1fr 140px 100px 100px 120px; align-items: center; gap: 16px; transition: background 0.2s;" onmouseover="this.style.background='rgba(255,255,255,0.02)'" onmouseout="this.style.background='transparent'">
                        <!-- Rank -->
                        <div style="font-size: ${isTop3 ? '24px' : '16px'}; font-weight: ${isTop3 ? '700' : '600'}; color: ${isTop3 ? '#fff' : 'rgba(255,255,255,0.4)'}; text-align: center;">
                            ${medalIcon || `#${index + 1}`}
                        </div>
                        
                        <!-- Country Info -->
                        <div style="display: flex; align-items: center; gap: 12px;">
                            <div style="width: 42px; height: 42px; background: rgba(255,255,255,0.05); border-radius: 10px; display: flex; align-items: center; justify-content: center; font-size: 22px;">
                                ${getCountryFlag(country.country_code)}
                            </div>
                            <div>
                                <div style="font-weight: 600; font-size: 14px; color: #fff; margin-bottom: 2px;">${country.country}</div>
                                <div style="font-size: 11px; color: rgba(255,255,255,0.4);">${country.country_code}</div>
                            </div>
                        </div>
                        
                        <!-- Leads with bar -->
                        <div>
                            <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 4px;">
                                <span style="font-size: 12px; color: rgba(255,255,255,0.5);">Leads</span>
                                <span style="font-weight: 700; color: #fff;">${country.leads}</span>
                            </div>
                            <div style="height: 4px; background: rgba(255,255,255,0.1); border-radius: 2px; overflow: hidden;">
                                <div style="height: 100%; width: ${leadsPercent}%; background: linear-gradient(90deg, #6366f1, #8b5cf6); border-radius: 2px;"></div>
                            </div>
                        </div>
                        
                        <!-- Converted -->
                        <div style="text-align: center;">
                            <div style="font-size: 11px; color: rgba(255,255,255,0.4); margin-bottom: 2px;">Convertidos</div>
                            <span style="display: inline-block; padding: 4px 12px; background: rgba(16, 185, 129, 0.15); color: #10b981; border-radius: 20px; font-weight: 600; font-size: 13px;">${country.converted}</span>
                        </div>
                        
                        <!-- Conversion Rate -->
                        <div style="text-align: center;">
                            <div style="font-size: 11px; color: rgba(255,255,255,0.4); margin-bottom: 2px;">Taxa</div>
                            <span style="color: ${conversionColor}; font-weight: 700; font-size: 14px;">${country.conversionRate.toFixed(1)}%</span>
                        </div>
                        
                        <!-- Revenue -->
                        <div style="text-align: right;">
                            <div style="font-size: 11px; color: rgba(255,255,255,0.4); margin-bottom: 2px;">Faturamento</div>
                            <span style="font-weight: 700; color: ${country.revenue > 0 ? '#10b981' : 'rgba(255,255,255,0.4)'}; font-size: 14px;">$${country.revenue.toLocaleString('en-US', { minimumFractionDigits: 2 })}</span>
                        </div>
                    </div>
                `;
            });
            
            container.innerHTML = html;
        }
        
        // Test geolocation API
        async function testGeolocationAPI() {
            try {
                showToast('Testando...', 'Verificando API de geolocalização', 'success');
                
                const response = await fetch(`${API_URL}/api/admin/test-geolocation?ip=8.8.8.8`, {
                    headers: { 'Authorization': `Bearer ${authToken}` }
                });
                
                const data = await response.json();
                console.log('Geolocation API Test:', data);
                
                if (data.success && data.result.country) {
                    showToast('API OK ✅', `Teste: ${data.result.country} (${data.result.country_code})`, 'success');
                } else {
                    showToast('API com problema', `Resultado: ${JSON.stringify(data.result)}`, 'warning');
                }
                
                // Show in alert for easier reading
                alert(`Teste da API de Geolocalização:\n\nIP testado: ${data.test_ip}\nPaís: ${data.result.country || 'NÃO ENCONTRADO'}\nCódigo: ${data.result.country_code || 'N/A'}\nCidade: ${data.result.city || 'N/A'}\n\nAPI Key: ${data.api_key_preview}`);
                
            } catch (error) {
                console.error('Test geolocation error:', error);
                showToast('Erro', 'Falha ao testar API', 'error');
            }
        }
        
        // Debug geo data for leads
        async function debugGeoData() {
            try {
                const response = await fetch(`${API_URL}/api/admin/leads/geo-debug`, {
                    headers: { 'Authorization': `Bearer ${authToken}` }
                });
                
                const data = await response.json();
                console.log('Geo Debug:', data);
                
                const summary = data.summary;
                let msg = `📊 Status dos Leads:\n\n`;
                msg += `Total de leads: ${summary.total}\n`;
                msg += `Com IP salvo: ${summary.with_ip}\n`;
                msg += `Com país: ${summary.with_country}\n`;
                msg += `Com código válido: ${summary.with_valid_country_code}\n\n`;
                msg += `📋 Amostra (últimos 5):\n`;
                
                data.sample_leads.slice(0, 5).forEach((lead, i) => {
                    msg += `${i+1}. IP: ${lead.ip || 'VAZIO'} → ${lead.country || 'Sem país'} (${lead.country_code || 'XX'})\n`;
                });
                
                alert(msg);
                
                showToast('Debug', `${summary.with_ip}/${summary.total} leads têm IP salvo`, summary.with_ip > 0 ? 'success' : 'warning');
                
            } catch (error) {
                console.error('Debug geo error:', error);
                showToast('Erro', 'Falha ao buscar dados de debug', 'error');
            }
        }
        
        // Enrich geolocation for leads without country data
        async function enrichGeolocation() {
            const btn = document.getElementById('enrichGeoBtn');
            const originalText = btn.innerHTML;
            btn.innerHTML = '<span class="spinner" style="width: 16px; height: 16px;"></span> Processando...';
            btn.disabled = true;
            
            try {
                const response = await fetch(`${API_URL}/api/admin/enrich-geolocation`, {
                    method: 'POST',
                    headers: { 'Authorization': `Bearer ${authToken}` }
                });
                
                const data = await response.json();
                console.log('Enrich result:', data);
                
                if (data.success) {
                    showToast('Geolocalização', `${data.enriched} leads atualizados. ${data.remaining} restantes.`, 'success');
                    
                    // Reload data
                    await loadCountriesData();
                    
                    // If there are more leads to process, show info
                    if (data.remaining > 0) {
                        showToast('Dica', `Clique novamente para processar mais ${data.remaining} leads.`, 'warning');
                    }
                } else {
                    showToast('Erro', data.error || 'Erro ao enriquecer dados', 'error');
                }
            } catch (error) {
                console.error('Error enriching geolocation:', error);
                showToast('Erro', 'Falha ao conectar com o servidor', 'error');
            } finally {
                btn.innerHTML = originalText;
                btn.disabled = false;
            }
        }
        
        // Load reports stats
        async function loadReportsStats() {
            try {
                const headers = { 'Authorization': `Bearer ${authToken}` };
                const [leadsRes, salesRes, refundsRes] = await Promise.all([
                    fetch(`${API_URL}/api/admin/leads`, { headers }),
                    fetch(`${API_URL}/api/admin/transactions`, { headers }),
                    fetch(`${API_URL}/api/admin/refunds`, { headers })
                ]);
                
                const leadsData = await leadsRes.json();
                const salesData = await salesRes.json();
                const refundsData = await refundsRes.json();
                
                // API returns { leads: [], pagination: {} } format
                const leadsCount = leadsData.pagination ? leadsData.pagination.total : (Array.isArray(leadsData) ? leadsData.length : 0);
                const salesCount = salesData.transactions ? salesData.transactions.length : (Array.isArray(salesData) ? salesData.length : 0);
                const refundsCount = refundsData.total || (refundsData.refunds ? refundsData.refunds.length : 0);
                
                document.getElementById('reportLeadsCount').textContent = leadsCount;
                document.getElementById('reportSalesCount').textContent = salesCount;
                document.getElementById('reportRefundsCount').textContent = refundsCount;
                
                // Set default dates
                const today = new Date();
                const firstDay = new Date(today.getFullYear(), today.getMonth(), 1);
                if (document.getElementById('exportDateEnd')) {
                    document.getElementById('exportDateEnd').value = today.toISOString().split('T')[0];
                    document.getElementById('exportDateStart').value = firstDay.toISOString().split('T')[0];
                }
                
                // Load refund requests table
                loadRefundRequests();
            } catch (error) {
                console.error('Error loading report stats:', error);
            }
        }
        
        // Load refund requests for the table
        async function loadRefundRequests() {
            try {
                const statusFilter = document.getElementById('refundStatusFilter')?.value || '';
                const statusParam = statusFilter ? `?status=${statusFilter}` : '';
                
                const response = await fetch(`${API_URL}/api/admin/refunds${statusParam}`, { 
                    headers: { 'Authorization': `Bearer ${authToken}` } 
                });
                
                if (response.status === 401 || response.status === 403) { logout(); return; }
                const data = await response.json();
                
                const refunds = data.refunds || [];
                document.getElementById('refundRequestsCount').textContent = `${refunds.length} pedidos`;
                
                renderRefundRequestsTable(refunds);
            } catch (error) {
                console.error('Error loading refund requests:', error);
                document.getElementById('refundRequestsTableBody').innerHTML = `
                    <tr><td colspan="8"><div class="empty-state"><h3>Erro ao carregar</h3></div></td></tr>
                `;
            }
        }
        
        // Render refund requests table
        function renderRefundRequestsTable(refunds) {
            if (!refunds || refunds.length === 0) {
                document.getElementById('refundRequestsTableBody').innerHTML = `
                    <tr><td colspan="8">
                        <div class="empty-state">
                            <div class="empty-state-icon">📋</div>
                            <h3>Nenhum pedido de reembolso</h3>
                            <p>Os pedidos aparecerão aqui quando clientes solicitarem.</p>
                        </div>
                    </td></tr>
                `;
                return;
            }
            
            const statusLabels = {
                'pending': { text: 'Pendente', class: 'badge-new', icon: '⏳' },
                'approved': { text: 'Aprovado', class: 'badge-converted', icon: '✅' },
                'rejected': { text: 'Rejeitado', class: 'badge-lost', icon: '❌' }
            };
            
            let html = '';
            refunds.forEach(refund => {
                const status = statusLabels[refund.status] || statusLabels['pending'];
                const date = new Date(refund.created_at).toLocaleDateString('pt-BR', { 
                    day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit' 
                });
                const reason = refund.reason || refund.refund_reason || '-';
                const product = refund.product || refund.product_name || '-';
                
                const refundData = {
                    name: refund.full_name || refund.name,
                    protocol: refund.protocol,
                    email: refund.email,
                    phone: refund.phone || refund.whatsapp,
                    product: product,
                    reason: reason
                };
                
                html += `<tr>
                    <td>${date}</td>
                    <td><strong>${refund.name || '-'}</strong></td>
                    <td class="text-primary">${refund.email || '-'}</td>
                    <td class="mono">${refund.whatsapp || refund.phone || '-'}</td>
                    <td>${product}</td>
                    <td style="max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${reason}">${reason}</td>
                    <td><span class="badge ${status.class}"><span class="badge-dot"></span>${status.icon} ${status.text}</span></td>
                    <td>
                        <div style="display: flex; gap: 4px; flex-wrap: wrap;">
                            <button class="action-btn" onclick="viewRefundDetails(${refund.id})" title="Ver detalhes completos">👁️</button>
                            ${refund.email ? `
                                <button class="action-btn" onclick='sendEmailTo(${JSON.stringify(refundData)}, "followUp")' title="Enviar Email" style="color: #3b82f6;">📧</button>
                            ` : ''}
                            ${(refund.phone || refund.whatsapp) ? `
                                <button class="action-btn" onclick='sendWhatsAppTo(${JSON.stringify(refundData)}, "followUp")' title="Enviar WhatsApp" style="color: #25d366;">💬</button>
                            ` : ''}
                            ${refund.status === 'pending' && refund.source === 'form' ? `
                                <button class="action-btn" onclick="updateRefundStatus(${refund.id}, 'approved')" title="Aprovar" style="color: var(--success);">✅</button>
                                <button class="action-btn" onclick="updateRefundStatus(${refund.id}, 'rejected')" title="Rejeitar" style="color: var(--danger);">❌</button>
                            ` : ''}
                        </div>
                    </td>
                </tr>`;
            });
            
            document.getElementById('refundRequestsTableBody').innerHTML = html;
        }
        
        // Email & WhatsApp Templates
        const communicationTemplates = {
            email: {
                requestInfo: {
                    subject: 'Solicitação de Reembolso - Protocolo {{protocol}}',
                    body: `Olá {{name}},

Recebemos sua solicitação de reembolso (Protocolo: {{protocol}}).

Para darmos continuidade ao processo, precisamos de algumas informações adicionais:

1. Motivo detalhado da solicitação
2. Comprovante de compra (se disponível)
3. Confirmação do email cadastrado: {{email}}

Aguardamos seu retorno para processar sua solicitação.

Atenciosamente,
Equipe de Suporte`
                },
                approved: {
                    subject: 'Reembolso Aprovado - Protocolo {{protocol}}',
                    body: `Olá {{name}},

Ótimas notícias! Seu reembolso foi APROVADO.

📋 Protocolo: {{protocol}}
💰 Produto: {{product}}
✅ Status: Aprovado

O valor será estornado em até 7 dias úteis, dependendo da operadora do cartão.

Se tiver alguma dúvida, estamos à disposição.

Atenciosamente,
Equipe de Suporte`
                },
                rejected: {
                    subject: 'Solicitação de Reembolso - Protocolo {{protocol}}',
                    body: `Olá {{name}},

Agradecemos por entrar em contato.

Após análise cuidadosa, infelizmente não podemos aprovar sua solicitação de reembolso neste momento.

📋 Protocolo: {{protocol}}
💰 Produto: {{product}}

Motivo: {{reason}}

Se você acredita que há um erro ou deseja mais esclarecimentos, por favor entre em contato conosco.

Atenciosamente,
Equipe de Suporte`
                },
                followUp: {
                    subject: 'Acompanhamento - Protocolo {{protocol}}',
                    body: `Olá {{name}},

Estamos entrando em contato para acompanhar sua solicitação de reembolso.

📋 Protocolo: {{protocol}}
💰 Produto: {{product}}

Há algo mais que possamos ajudar?

Atenciosamente,
Equipe de Suporte`
                }
            },
            whatsapp: {
                requestInfo: `Olá {{name}}! 👋

Recebemos sua solicitação de reembolso (Protocolo: *{{protocol}}*).

Para continuar, precisamos de:
✅ Motivo detalhado
✅ Comprovante de compra
✅ Confirmação do email: {{email}}

Aguardo seu retorno! 😊`,
                approved: `Olá {{name}}! 🎉

Ótimas notícias! Seu reembolso foi *APROVADO*!

📋 Protocolo: *{{protocol}}*
💰 Produto: {{product}}
✅ Status: Aprovado

O valor será estornado em até 7 dias úteis.

Qualquer dúvida, estou aqui! 😊`,
                rejected: `Olá {{name}},

Após análise, infelizmente não podemos aprovar seu reembolso neste momento.

📋 Protocolo: *{{protocol}}*
💰 Produto: {{product}}

Se tiver dúvidas, estou à disposição para conversar.`,
                followUp: `Olá {{name}}! 👋

Passando para acompanhar sua solicitação de reembolso.

📋 Protocolo: *{{protocol}}*
💰 Produto: {{product}}

Posso ajudar em algo mais? 😊`
            }
        };

        // Replace template variables
        function fillTemplate(template, data) {
            return template
                .replace(/\{\{name\}\}/g, data.name || 'Cliente')
                .replace(/\{\{protocol\}\}/g, data.protocol || 'N/A')
                .replace(/\{\{email\}\}/g, data.email || '')
                .replace(/\{\{phone\}\}/g, data.phone || '')
                .replace(/\{\{product\}\}/g, data.product || 'Produto')
                .replace(/\{\{reason\}\}/g, data.reason || 'Não especificado');
        }

        // Open Gmail with pre-filled email
        function sendEmailTo(refund, templateKey) {
            const template = communicationTemplates.email[templateKey];
            if (!template) return;
            
            const subject = fillTemplate(template.subject, refund);
            const body = fillTemplate(template.body, refund);
            
            const gmailUrl = `https://mail.google.com/mail/?view=cm&fs=1&to=${encodeURIComponent(refund.email)}&su=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`;
            
            window.open(gmailUrl, '_blank');
            showToast('Email', 'Gmail aberto com mensagem pronta', 'success');
        }

        // Open WhatsApp with pre-filled message
        function sendWhatsAppTo(refund, templateKey) {
            const template = communicationTemplates.whatsapp[templateKey];
            if (!template) return;
            
            const message = fillTemplate(template, refund);
            
            // Clean phone number (remove non-digits)
            let phone = (refund.phone || refund.whatsapp || '').replace(/\D/g, '');
            
            if (!phone) {
                showToast('Erro', 'Número de telefone não disponível', 'error');
                return;
            }
            
            // Add country code if not present
            if (!phone.startsWith('55') && phone.length <= 11) {
                phone = '55' + phone;
            }
            
            const whatsappUrl = `https://wa.me/${phone}?text=${encodeURIComponent(message)}`;
            
            window.open(whatsappUrl, '_blank');
            showToast('WhatsApp', 'WhatsApp aberto com mensagem pronta', 'success');
        }

        // View refund details with quick actions
        async function viewRefundDetails(id) {
            try {
                const response = await fetch(`${API_URL}/api/admin/refunds`, {
                    headers: { 'Authorization': `Bearer ${token}` }
                });
                const data = await response.json();
                const refund = data.refunds.find(r => r.id === id);
                
                if (!refund) {
                    showToast('Erro', 'Reembolso não encontrado', 'error');
                    return;
                }
                
                // Prepare refund data for templates
                const refundData = {
                    name: refund.full_name || refund.name,
                    protocol: refund.protocol,
                    email: refund.email,
                    phone: refund.phone || refund.whatsapp,
                    product: refund.product,
                    reason: refund.reason
                };
                
                // Build modal content
                const statusMap = {
                    'pending': { text: 'Pendente', class: 'warning', icon: '⏳' },
                    'approved': { text: 'Aprovado', class: 'success', icon: '✅' },
                    'rejected': { text: 'Rejeitado', class: 'danger', icon: '❌' }
                };
                const status = statusMap[refund.status] || statusMap.pending;
                
                const sourceMap = {
                    'form': { text: 'Página de Captura', icon: '📝', color: '#3b82f6' },
                    'monetizze': { text: 'Monetizze', icon: '💸', color: '#f59e0b' }
                };
                const source = sourceMap[refund.source] || sourceMap.form;
                
                const typeMap = {
                    'refund': { text: 'Reembolso', icon: '💸' },
                    'chargeback': { text: 'Chargeback', icon: '⚠️' }
                };
                const type = typeMap[refund.refund_type] || typeMap.refund;
                
                document.getElementById('refundModalBody').innerHTML = `
                    <div style="display: grid; gap: 20px;">
                        <!-- Header Info -->
                        <div style="display: flex; justify-content: space-between; align-items: start; padding: 16px; background: var(--bg-tertiary); border-radius: 12px;">
                            <div>
                                <div style="font-size: 12px; color: var(--text-muted); margin-bottom: 4px;">PROTOCOLO</div>
                                <div style="font-size: 18px; font-weight: 700; color: var(--primary); font-family: monospace;">${refund.protocol}</div>
                            </div>
                            <div style="text-align: right;">
                                <span class="status-badge ${status.class}">${status.icon} ${status.text}</span>
                                <div style="font-size: 11px; color: var(--text-muted); margin-top: 4px;">
                                    ${new Date(refund.created_at).toLocaleString('pt-BR')}
                                </div>
                            </div>
                        </div>
                        
                        <!-- Source & Type -->
                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px;">
                            <div style="padding: 12px; background: var(--bg-tertiary); border-radius: 8px; border-left: 3px solid ${source.color};">
                                <div style="font-size: 11px; color: var(--text-muted); margin-bottom: 4px;">ORIGEM</div>
                                <div style="font-weight: 600; color: var(--text-primary);">${source.icon} ${source.text}</div>
                            </div>
                            <div style="padding: 12px; background: var(--bg-tertiary); border-radius: 8px;">
                                <div style="font-size: 11px; color: var(--text-muted); margin-bottom: 4px;">TIPO</div>
                                <div style="font-weight: 600; color: var(--text-primary);">${type.icon} ${type.text}</div>
                            </div>
                        </div>
                        
                        <!-- Customer Info -->
                        <div style="padding: 16px; background: var(--bg-tertiary); border-radius: 12px;">
                            <h4 style="margin: 0 0 12px 0; color: var(--text-primary); font-size: 14px;">👤 Informações do Cliente</h4>
                            <div style="display: grid; gap: 8px;">
                                <div style="display: flex; justify-content: space-between;">
                                    <span style="color: var(--text-muted);">Nome:</span>
                                    <strong style="color: var(--text-primary);">${refund.full_name || refund.name || '-'}</strong>
                                </div>
                                <div style="display: flex; justify-content: space-between;">
                                    <span style="color: var(--text-muted);">Email:</span>
                                    <strong style="color: var(--primary); font-family: monospace; font-size: 13px;">${refund.email || '-'}</strong>
                                </div>
                                <div style="display: flex; justify-content: space-between;">
                                    <span style="color: var(--text-muted);">Telefone:</span>
                                    <strong style="color: var(--text-primary); font-family: monospace;">${refund.phone || refund.whatsapp || '-'}</strong>
                                </div>
                            </div>
                        </div>
                        
                        <!-- Product & Value -->
                        <div style="display: grid; grid-template-columns: 2fr 1fr; gap: 12px;">
                            <div style="padding: 12px; background: var(--bg-tertiary); border-radius: 8px;">
                                <div style="font-size: 11px; color: var(--text-muted); margin-bottom: 4px;">PRODUTO</div>
                                <div style="font-weight: 600; color: var(--text-primary);">${refund.product || '-'}</div>
                            </div>
                            ${refund.value ? `
                                <div style="padding: 12px; background: var(--bg-tertiary); border-radius: 8px;">
                                    <div style="font-size: 11px; color: var(--text-muted); margin-bottom: 4px;">VALOR</div>
                                    <div style="font-weight: 700; color: var(--success); font-size: 16px;">R$ ${parseFloat(refund.value).toFixed(2)}</div>
                                </div>
                            ` : ''}
                        </div>
                        
                        <!-- Reason -->
                        <div style="padding: 16px; background: var(--bg-tertiary); border-radius: 12px;">
                            <h4 style="margin: 0 0 8px 0; color: var(--text-primary); font-size: 14px;">💬 Motivo</h4>
                            <p style="margin: 0; color: var(--text-secondary); line-height: 1.6; white-space: pre-wrap;">${refund.reason || 'Não especificado'}</p>
                        </div>
                        
                        <!-- Quick Actions -->
                        ${refund.email || refund.phone || refund.whatsapp ? `
                            <div style="padding: 20px; background: linear-gradient(135deg, rgba(59, 130, 246, 0.1), rgba(16, 185, 129, 0.1)); border-radius: 12px; border: 1px solid rgba(59, 130, 246, 0.2);">
                                <h4 style="margin: 0 0 16px 0; color: var(--text-primary); font-size: 15px; font-weight: 600;">⚡ Ações Rápidas de Comunicação</h4>
                                
                                <!-- Email Actions -->
                                ${refund.email ? `
                                    <div style="margin-bottom: 16px;">
                                        <div style="font-size: 12px; color: var(--text-muted); margin-bottom: 8px; font-weight: 500;">📧 ENVIAR EMAIL</div>
                                        <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 8px;">
                                            <button class="btn-secondary" onclick='sendEmailTo(${JSON.stringify(refundData)}, "requestInfo")' style="font-size: 13px; padding: 10px;">
                                                📝 Solicitar Info
                                            </button>
                                            <button class="btn-secondary" onclick='sendEmailTo(${JSON.stringify(refundData)}, "followUp")' style="font-size: 13px; padding: 10px;">
                                                👋 Acompanhamento
                                            </button>
                                            <button class="btn-secondary" onclick='sendEmailTo(${JSON.stringify(refundData)}, "approved")' style="font-size: 13px; padding: 10px; background: rgba(34, 197, 94, 0.1); border-color: rgba(34, 197, 94, 0.3); color: var(--success);">
                                                ✅ Aprovado
                                            </button>
                                            <button class="btn-secondary" onclick='sendEmailTo(${JSON.stringify(refundData)}, "rejected")' style="font-size: 13px; padding: 10px; background: rgba(239, 68, 68, 0.1); border-color: rgba(239, 68, 68, 0.3); color: var(--danger);">
                                                ❌ Rejeitado
                                            </button>
                                        </div>
                                    </div>
                                ` : ''}
                                
                                <!-- WhatsApp Actions -->
                                ${refund.phone || refund.whatsapp ? `
                                    <div>
                                        <div style="font-size: 12px; color: var(--text-muted); margin-bottom: 8px; font-weight: 500;">💬 ENVIAR WHATSAPP</div>
                                        <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 8px;">
                                            <button class="btn-secondary" onclick='sendWhatsAppTo(${JSON.stringify(refundData)}, "requestInfo")' style="font-size: 13px; padding: 10px; background: rgba(37, 211, 102, 0.1); border-color: rgba(37, 211, 102, 0.3); color: #25d366;">
                                                📝 Solicitar Info
                                            </button>
                                            <button class="btn-secondary" onclick='sendWhatsAppTo(${JSON.stringify(refundData)}, "followUp")' style="font-size: 13px; padding: 10px; background: rgba(37, 211, 102, 0.1); border-color: rgba(37, 211, 102, 0.3); color: #25d366;">
                                                👋 Acompanhamento
                                            </button>
                                            <button class="btn-secondary" onclick='sendWhatsAppTo(${JSON.stringify(refundData)}, "approved")' style="font-size: 13px; padding: 10px; background: rgba(37, 211, 102, 0.1); border-color: rgba(37, 211, 102, 0.3); color: #25d366;">
                                                ✅ Aprovado
                                            </button>
                                            <button class="btn-secondary" onclick='sendWhatsAppTo(${JSON.stringify(refundData)}, "rejected")' style="font-size: 13px; padding: 10px; background: rgba(37, 211, 102, 0.1); border-color: rgba(37, 211, 102, 0.3); color: #25d366;">
                                                ❌ Rejeitado
                                            </button>
                                        </div>
                                    </div>
                                ` : '<p style="font-size: 13px; color: var(--text-muted); text-align: center; margin: 0;">Telefone não disponível para WhatsApp</p>'}
                            </div>
                        ` : ''}
                    </div>
                `;
                
                // Build footer actions
                let footerHtml = '<div style="display: flex; gap: 12px; justify-content: flex-end;">';
                
                if (refund.status === 'pending' && refund.source === 'form') {
                    footerHtml += `
                        <button class="btn-secondary" onclick="updateRefundStatus(${id}, 'approved')" style="background: var(--success); color: white; border-color: var(--success);">
                            ✅ Aprovar Reembolso
                        </button>
                        <button class="btn-secondary" onclick="updateRefundStatus(${id}, 'rejected')" style="background: var(--danger); color: white; border-color: var(--danger);">
                            ❌ Rejeitar
                        </button>
                    `;
                }
                
                footerHtml += '<button class="btn-secondary" onclick="closeRefundModal()">Fechar</button></div>';
                
                document.getElementById('refundModalFooter').innerHTML = footerHtml;
                document.getElementById('refundModal').style.display = 'flex';
                
            } catch (error) {
                console.error('Error loading refund details:', error);
                showToast('Erro', 'Falha ao carregar detalhes', 'error');
            }
        }
        
        function closeRefundModal() {
            document.getElementById('refundModal').style.display = 'none';
        }
        
        // Update refund status
        async function updateRefundStatus(id, newStatus) {
            const confirmMsg = newStatus === 'approved' 
                ? 'Tem certeza que deseja APROVAR este reembolso?' 
                : 'Tem certeza que deseja REJEITAR este reembolso?';
            
            if (!confirm(confirmMsg)) return;
            
            try {
                const response = await fetch(`${API_URL}/api/admin/refunds/${id}`, {
                    method: 'PUT',
                    headers: { 
                        'Content-Type': 'application/json',
                        'Authorization': `Bearer ${authToken}` 
                    },
                    body: JSON.stringify({ status: newStatus, notes: '' })
                });
                
                if (response.ok) {
                    showToast('Sucesso!', `Reembolso ${newStatus === 'approved' ? 'aprovado' : 'rejeitado'} com sucesso`, 'success');
                    loadRefundRequests();
                    loadReportsStats();
                } else {
                    showToast('Erro', 'Não foi possível atualizar o status', 'error');
                }
            } catch (error) {
                console.error('Error updating refund:', error);
                showToast('Erro', 'Falha ao atualizar reembolso', 'error');
            }
        }
        
        // Toggle custom dates visibility
        function toggleCustomDates() {
            const period = document.getElementById('exportPeriod').value;
            const customRow = document.getElementById('customDatesRow');
            customRow.style.display = period === 'custom' ? 'grid' : 'none';
        }
        
        // Select export format
        let selectedFormat = 'excel';
        function selectFormat(format) {
            selectedFormat = format;
            document.querySelectorAll('.export-format-option').forEach(opt => {
                opt.classList.toggle('selected', opt.dataset.format === format);
            });
        }
        
        // Execute export
        async function executeExport() {
            const dataType = document.getElementById('exportDataType').value;
            const period = document.getElementById('exportPeriod').value;
            
            let dateStart = null, dateEnd = null;
            const today = new Date();
            
            if (period === 'today') {
                dateStart = dateEnd = today.toISOString().split('T')[0];
            } else if (period === 'week') {
                dateEnd = today.toISOString().split('T')[0];
                const weekAgo = new Date(today);
                weekAgo.setDate(weekAgo.getDate() - 7);
                dateStart = weekAgo.toISOString().split('T')[0];
            } else if (period === 'month') {
                dateEnd = today.toISOString().split('T')[0];
                const monthAgo = new Date(today);
                monthAgo.setDate(monthAgo.getDate() - 30);
                dateStart = monthAgo.toISOString().split('T')[0];
            } else if (period === 'custom') {
                dateStart = document.getElementById('exportDateStart').value;
                dateEnd = document.getElementById('exportDateEnd').value;
                if (!dateStart || !dateEnd) {
                    showToast('Erro', 'Selecione as datas do período', 'error');
                    return;
                }
            }
            
            // Call the appropriate export function
            if (selectedFormat === 'excel') exportToExcel(dataType, dateStart, dateEnd);
            else if (selectedFormat === 'pdf') exportToPDF(dataType, dateStart, dateEnd);
            else if (selectedFormat === 'csv') exportToCSV(dataType, dateStart, dateEnd);
        }
        
        // Get export data
        async function getExportData(type) {
            const headers = { 'Authorization': `Bearer ${authToken}` };
            let data = {};
            
            try {
                if (type === 'leads' || type === 'all') {
                    const res = await fetch(`${API_URL}/api/admin/leads?limit=10000`, { headers });
                    const leadsData = await res.json();
                    // API returns { leads: [], pagination: {} }
                    data.leads = leadsData.leads || leadsData;
                }
                if (type === 'sales' || type === 'all') {
                    const res = await fetch(`${API_URL}/api/admin/transactions`, { headers });
                    data.sales = await res.json();
                }
                if (type === 'refunds' || type === 'all') {
                    const res = await fetch(`${API_URL}/api/admin/refunds`, { headers });
                    data.refunds = await res.json();
                }
                if (type === 'funnel' || type === 'all') {
                    const res = await fetch(`${API_URL}/api/admin/stats`, { headers });
                    data.funnel = await res.json();
                }
                return data;
            } catch (error) {
                console.error('Error fetching export data:', error);
                throw error;
            }
        }
        
        // Export to Excel
        async function exportToExcel(type, dateStart = null, dateEnd = null) {
            try {
                showToast('Gerando...', 'Preparando arquivo Excel', 'success');
                
                const data = await getExportData(type);
                const wb = XLSX.utils.book_new();
                const today = new Date().toISOString().split('T')[0];
                
                if (data.leads) {
                    const leadsData = data.leads.map(l => ({
                        'Nome': l.name || '-',
                        'Email': l.email,
                        'WhatsApp': l.whatsapp,
                        'Alvo': l.target_phone,
                        'Status': l.status,
                        'Tags': l.tags || '-',
                        'Data': new Date(l.created_at).toLocaleString('pt-BR'),
                        'Observações': l.notes || '-'
                    }));
                    const ws = XLSX.utils.json_to_sheet(leadsData);
                    ws['!cols'] = [{wch:20},{wch:30},{wch:18},{wch:18},{wch:12},{wch:15},{wch:18},{wch:30}];
                    XLSX.utils.book_append_sheet(wb, ws, 'Leads');
                }
                
                if (data.sales) {
                    const salesData = data.sales.map(s => ({
                        'Código': s.transaction_id,
                        'Produto': s.product_name,
                        'Valor': `R$ ${parseFloat(s.amount || 0).toFixed(2)}`,
                        'Status': s.status,
                        'Cliente': s.customer_name || '-',
                        'Email': s.customer_email || '-',
                        'Data': new Date(s.created_at).toLocaleString('pt-BR')
                    }));
                    const ws = XLSX.utils.json_to_sheet(salesData);
                    ws['!cols'] = [{wch:15},{wch:25},{wch:12},{wch:12},{wch:25},{wch:30},{wch:18}];
                    XLSX.utils.book_append_sheet(wb, ws, 'Vendas');
                }
                
                if (data.refunds) {
                    const refundsData = data.refunds.map(r => ({
                        'Protocolo': r.protocol,
                        'Nome': r.full_name,
                        'Email': r.email,
                        'Telefone': r.phone || '-',
                        'Motivo': r.reason,
                        'Status': r.status,
                        'Data Compra': r.purchase_date,
                        'Data Solicitação': new Date(r.created_at).toLocaleString('pt-BR'),
                        'Descrição': r.details || '-'
                    }));
                    const ws = XLSX.utils.json_to_sheet(refundsData);
                    ws['!cols'] = [{wch:15},{wch:25},{wch:30},{wch:18},{wch:25},{wch:12},{wch:12},{wch:18},{wch:40}];
                    XLSX.utils.book_append_sheet(wb, ws, 'Reembolsos');
                }
                
                if (data.funnel) {
                    const stats = data.funnel;
                    const funnelData = [
                        { 'Métrica': 'Total de Leads', 'Valor': stats.totalLeads || 0 },
                        { 'Métrica': 'Leads Hoje', 'Valor': stats.todayLeads || 0 },
                        { 'Métrica': 'Taxa de Conversão', 'Valor': `${stats.conversionRate || 0}%` },
                        { 'Métrica': 'Total de Vendas', 'Valor': stats.totalSales || 0 },
                        { 'Métrica': 'Receita Total', 'Valor': `R$ ${parseFloat(stats.totalRevenue || 0).toFixed(2)}` },
                        { 'Métrica': 'Ticket Médio', 'Valor': `R$ ${parseFloat(stats.avgTicket || 0).toFixed(2)}` },
                    ];
                    const ws = XLSX.utils.json_to_sheet(funnelData);
                    ws['!cols'] = [{wch:25},{wch:20}];
                    XLSX.utils.book_append_sheet(wb, ws, 'Métricas');
                }
                
                const fileName = type === 'all' ? `relatorio-completo-${today}.xlsx` : `${type}-${today}.xlsx`;
                XLSX.writeFile(wb, fileName);
                showToast('Sucesso!', 'Arquivo Excel baixado', 'success');
            } catch (error) {
                console.error('Excel export error:', error);
                showToast('Erro', 'Falha ao exportar Excel', 'error');
            }
        }
        
        // Export to PDF
        async function exportToPDF(type, dateStart = null, dateEnd = null) {
            try {
                showToast('Gerando...', 'Preparando arquivo PDF', 'success');
                
                const data = await getExportData(type);
                
                // Check if jsPDF is loaded
                if (typeof window.jspdf === 'undefined') {
                    showToast('Erro', 'Biblioteca PDF não carregada. Recarregue a página.', 'error');
                    return;
                }
                
                const { jsPDF } = window.jspdf;
                const doc = new jsPDF('l', 'mm', 'a4');
                const today = new Date().toLocaleDateString('pt-BR');
                const pageWidth = doc.internal.pageSize.getWidth();
                let currentPage = 1;
                let hasContent = false;
                
                // Header function
                const addHeader = (title) => {
                    doc.setFillColor(5, 150, 105);
                    doc.rect(0, 0, pageWidth, 25, 'F');
                    doc.setTextColor(255, 255, 255);
                    doc.setFontSize(18);
                    doc.setFont('helvetica', 'bold');
                    doc.text('ZapSpy.ai - ' + title, 14, 16);
                    doc.setFontSize(10);
                    doc.setFont('helvetica', 'normal');
                    doc.text('Gerado em: ' + today, pageWidth - 50, 16);
                    doc.setTextColor(0, 0, 0);
                };
                
                // Simple table function (no plugin needed)
                const drawTable = (headers, rows, startY) => {
                    const colWidth = (pageWidth - 28) / headers.length;
                    let y = startY;
                    
                    // Header row
                    doc.setFillColor(5, 150, 105);
                    doc.rect(14, y, pageWidth - 28, 10, 'F');
                    doc.setTextColor(255, 255, 255);
                    doc.setFontSize(10);
                    doc.setFont('helvetica', 'bold');
                    headers.forEach((h, i) => {
                        doc.text(String(h).substring(0, 20), 16 + (i * colWidth), y + 7);
                    });
                    y += 12;
                    
                    // Data rows
                    doc.setTextColor(0, 0, 0);
                    doc.setFont('helvetica', 'normal');
                    doc.setFontSize(9);
                    
                    rows.forEach((row, rowIndex) => {
                        if (y > 180) {
                            doc.addPage();
                            currentPage++;
                            y = 20;
                        }
                        
                        // Alternate row background
                        if (rowIndex % 2 === 0) {
                            doc.setFillColor(245, 245, 245);
                            doc.rect(14, y - 4, pageWidth - 28, 8, 'F');
                        }
                        
                        row.forEach((cell, i) => {
                            const text = String(cell || '-').substring(0, 25);
                            doc.text(text, 16 + (i * colWidth), y);
                        });
                        y += 8;
                    });
                    
                    return y;
                };
                
                // Leads
                if (data.leads && Array.isArray(data.leads) && data.leads.length > 0) {
                    addHeader('Relatório de Leads');
                    const leadsTable = data.leads.map(l => [
                        l.name || '-',
                        l.email || '-',
                        l.whatsapp || '-',
                        l.target_phone || '-',
                        l.status || '-',
                        l.created_at ? new Date(l.created_at).toLocaleDateString('pt-BR') : '-'
                    ]);
                    drawTable(['Nome', 'Email', 'WhatsApp', 'Alvo', 'Status', 'Data'], leadsTable, 35);
                    hasContent = true;
                    if (type === 'all') { doc.addPage(); currentPage++; }
                }
                
                // Sales
                if (data.sales && Array.isArray(data.sales) && data.sales.length > 0) {
                    addHeader('Relatório de Vendas');
                    const salesTable = data.sales.map(s => [
                        s.transaction_id || '-',
                        s.product_name || '-',
                        'R$ ' + parseFloat(s.amount || 0).toFixed(2),
                        s.status || '-',
                        s.customer_name || '-',
                        s.created_at ? new Date(s.created_at).toLocaleDateString('pt-BR') : '-'
                    ]);
                    drawTable(['Código', 'Produto', 'Valor', 'Status', 'Cliente', 'Data'], salesTable, 35);
                    hasContent = true;
                    if (type === 'all') { doc.addPage(); currentPage++; }
                }
                
                // Refunds
                if (data.refunds && Array.isArray(data.refunds) && data.refunds.length > 0) {
                    addHeader('Relatório de Reembolsos');
                    const refundsTable = data.refunds.map(r => [
                        r.protocol || '-',
                        r.full_name || '-',
                        r.email || '-',
                        r.reason || '-',
                        r.status || '-',
                        r.created_at ? new Date(r.created_at).toLocaleDateString('pt-BR') : '-'
                    ]);
                    drawTable(['Protocolo', 'Nome', 'Email', 'Motivo', 'Status', 'Data'], refundsTable, 35);
                    hasContent = true;
                    if (type === 'all' && data.funnel) { doc.addPage(); currentPage++; }
                }
                
                // Funnel metrics
                if (data.funnel) {
                    addHeader('Métricas do Funil');
                    const stats = data.funnel;
                    const metricsTable = [
                        ['Total de Leads', String(stats.totalLeads || 0)],
                        ['Leads Hoje', String(stats.todayLeads || 0)],
                        ['Taxa de Conversão', (stats.conversionRate || 0) + '%'],
                        ['Total de Vendas', String(stats.totalSales || 0)],
                        ['Receita Total', 'R$ ' + parseFloat(stats.totalRevenue || 0).toFixed(2)],
                        ['Ticket Médio', 'R$ ' + parseFloat(stats.avgTicket || 0).toFixed(2)]
                    ];
                    drawTable(['Métrica', 'Valor'], metricsTable, 35);
                    hasContent = true;
                }
                
                // Check if we have any content
                if (!hasContent) {
                    addHeader('Relatório');
                    doc.setFontSize(14);
                    doc.setTextColor(128, 128, 128);
                    doc.text('Nenhum dado encontrado para exportar.', pageWidth / 2, 80, { align: 'center' });
                }
                
                const fileName = type === 'all' ? 'relatorio-completo-' + new Date().toISOString().split('T')[0] + '.pdf' : type + '-' + new Date().toISOString().split('T')[0] + '.pdf';
                doc.save(fileName);
                showToast('Sucesso!', 'Arquivo PDF baixado', 'success');
            } catch (error) {
                console.error('PDF export error:', error);
                showToast('Erro', error.message || 'Falha ao exportar PDF', 'error');
            }
        }
        
        // Export to CSV
        async function exportToCSV(type, dateStart = null, dateEnd = null) {
            try {
                showToast('Gerando...', 'Preparando arquivo CSV', 'success');
                
                const data = await getExportData(type);
                let csvContent = '';
                let fileName = '';
                
                if (type === 'leads' && data.leads) {
                    csvContent = 'Nome,Email,WhatsApp,Alvo,Status,Tags,Data,Observações\n';
                    data.leads.forEach(l => {
                        csvContent += `"${l.name || ''}","${l.email}","${l.whatsapp || ''}","${l.target_phone || ''}","${l.status}","${l.tags || ''}","${new Date(l.created_at).toLocaleString('pt-BR')}","${(l.notes || '').replace(/"/g, '""')}"\n`;
                    });
                    fileName = `leads-${new Date().toISOString().split('T')[0]}.csv`;
                }
                
                if (type === 'sales' && data.sales) {
                    csvContent = 'Código,Produto,Valor,Status,Cliente,Email,Data\n';
                    data.sales.forEach(s => {
                        csvContent += `"${s.transaction_id}","${s.product_name || ''}","R$ ${parseFloat(s.amount || 0).toFixed(2)}","${s.status}","${s.customer_name || ''}","${s.customer_email || ''}","${new Date(s.created_at).toLocaleString('pt-BR')}"\n`;
                    });
                    fileName = `vendas-${new Date().toISOString().split('T')[0]}.csv`;
                }
                
                if (type === 'refunds' && data.refunds) {
                    csvContent = 'Protocolo,Nome,Email,Telefone,Motivo,Status,Data Compra,Data Solicitação,Descrição\n';
                    data.refunds.forEach(r => {
                        csvContent += `"${r.protocol}","${r.full_name}","${r.email}","${r.phone || ''}","${r.reason}","${r.status}","${r.purchase_date}","${new Date(r.created_at).toLocaleString('pt-BR')}","${(r.details || '').replace(/"/g, '""')}"\n`;
                    });
                    fileName = `reembolsos-${new Date().toISOString().split('T')[0]}.csv`;
                }
                
                // Create download
                const blob = new Blob(['\ufeff' + csvContent], { type: 'text/csv;charset=utf-8;' });
                const url = window.URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = fileName;
                a.click();
                window.URL.revokeObjectURL(url);
                
                showToast('Sucesso!', 'Arquivo CSV baixado', 'success');
            } catch (error) {
                console.error('CSV export error:', error);
                showToast('Erro', 'Falha ao exportar CSV', 'error');
            }
        }
        
        function copyPostbackUrl() {
            navigator.clipboard.writeText(document.getElementById('postbackUrl').textContent);
            showToast('Copiado!', 'URL na área de transferência', 'success');
        }
        
        async function viewPostbackDebug() {
            try {
                showToast('Carregando...', 'Buscando postbacks recentes', 'success');
                
                const response = await fetch(`${API_URL}/api/admin/debug/postbacks`, {
                    headers: { 'Authorization': `Bearer ${authToken}` }
                });
                
                if (!response.ok) throw new Error('Falha ao buscar dados');
                
                const data = await response.json();
                
                const resultDiv = document.getElementById('postbackDebugResult');
                const contentPre = document.getElementById('postbackDebugContent');
                
                const memoryCount = data.memoryCount || 0;
                const dbLogCount = data.dbLogCount || 0;
                const totalCount = memoryCount + dbLogCount;
                
                if (totalCount === 0) {
                    contentPre.textContent = '❌ Nenhum postback recebido ainda.\n\nFaça uma venda de teste na Monetizze para ver os dados aqui.';
                } else {
                    let output = `📊 Postbacks: ${memoryCount} em memória, ${dbLogCount} salvos no banco\n`;
                    output += `ℹ️ ${data.info}\n\n`;
                    
                    // Show recent transactions first
                    if (data.recentTransactions && data.recentTransactions.length > 0) {
                        output += '═══════════════════════════════════════════════════════════\n';
                        output += '💾 TRANSAÇÕES RECENTES (salvas no banco):\n';
                        output += '═══════════════════════════════════════════════════════════\n\n';
                        data.recentTransactions.forEach((tx, i) => {
                            output += `🔸 TX #${i + 1}: ${tx.transaction_id}\n`;
                            output += `   Email: ${tx.email}\n`;
                            output += `   Produto: ${tx.product}\n`;
                            output += `   Valor: R$ ${tx.value}\n`;
                            output += `   Status: ${tx.status} (Monetizze: ${tx.monetizze_status})\n`;
                            output += `   Data: ${new Date(tx.created_at).toLocaleString('pt-BR')}\n\n`;
                        });
                    }
                    
                    // Show memory postbacks
                    if (data.postbacks && data.postbacks.length > 0) {
                        output += '═══════════════════════════════════════════════════════════\n';
                        output += '📥 POSTBACKS EM MEMÓRIA (perdidos ao reiniciar servidor):\n';
                        output += '═══════════════════════════════════════════════════════════\n\n';
                        data.postbacks.forEach((p, i) => {
                            output += `🔹 POSTBACK #${i + 1}\n`;
                            output += `   Timestamp: ${p.timestamp}\n`;
                            output += `   Transação: ${p.chave_unica || 'N/A'}\n`;
                            output += `   Produto: ${p.produto || 'N/A'}\n`;
                            output += `   Status: ${p.status || 'N/A'}\n`;
                            output += `   Email: ${p.comprador || 'N/A'}\n\n`;
                            output += `   💰 CAMPOS DE VALOR:\n`;
                            Object.entries(p.valores || {}).forEach(([key, val]) => {
                                const marker = val !== 'N/A' ? '✅' : '  ';
                                output += `   ${marker} ${key}: ${val}\n`;
                            });
                            output += '\n─────────────────────────────────────────\n\n';
                        });
                    }
                    
                    // Show DB logs (raw postbacks)
                    if (data.dbLogs && data.dbLogs.length > 0) {
                        output += '═══════════════════════════════════════════════════════════\n';
                        output += '📋 LOGS DE POSTBACK (banco de dados):\n';
                        output += '═══════════════════════════════════════════════════════════\n\n';
                        data.dbLogs.slice(0, 10).forEach((log, i) => {
                            const body = typeof log.body === 'string' ? JSON.parse(log.body) : log.body;
                            output += `📝 LOG #${i + 1} (${new Date(log.created_at).toLocaleString('pt-BR')})\n`;
                            output += `   Content-Type: ${log.content_type}\n`;
                            output += `   Produto: ${body?.produto?.nome || body?.['produto.nome'] || 'N/A'}\n`;
                            output += `   Email: ${body?.comprador?.email || body?.['comprador.email'] || 'N/A'}\n`;
                            output += `   Keys: ${Object.keys(body || {}).slice(0, 10).join(', ')}...\n\n`;
                        });
                    }
                    
                    contentPre.textContent = output;
                }
                
                resultDiv.style.display = 'block';
                showToast('Sucesso!', `${totalCount} postback(s) encontrado(s)`, 'success');
                
            } catch (error) {
                console.error('Error loading postback debug:', error);
                showToast('Erro', 'Falha ao carregar postbacks', 'error');
            }
        }
        
        async function testPostbackEndpoint() {
            try {
                const response = await fetch(`${API_URL}/api/postback/monetizze`, {
                    method: 'GET'
                });
                
                const data = await response.json();
                
                if (data.status === 'ok') {
                    showToast('✅ Endpoint OK!', data.message, 'success');
                } else {
                    showToast('⚠️ Resposta', JSON.stringify(data), 'warning');
                }
            } catch (error) {
                showToast('❌ Erro', 'Endpoint não está respondendo', 'error');
            }
        }
        
        // Test Monetizze API connectivity
        async function testMonetizzeApi() {
            showToast('🔍 Testando...', 'Verificando conectividade com a API Monetizze', 'info');
            
            try {
                const response = await fetch(`${API_URL}/api/admin/test-monetizze-api`, {
                    method: 'GET',
                    headers: {
                        'Authorization': `Bearer ${authToken}`
                    }
                });
                
                const data = await response.json();
                
                // Show results in the sync result area
                const resultDiv = document.getElementById('syncMonetizzeResult');
                const contentDiv = document.getElementById('syncMonetizzeContent');
                
                resultDiv.style.display = 'block';
                
                let html = '🔍 TESTE DE CONECTIVIDADE API MONETIZZE\n';
                html += '=' .repeat(50) + '\n\n';
                html += `Consumer Key configurada: ${data.consumerKeyPresent ? '✅ Sim' : '❌ Não'}\n\n`;
                
                if (data.tests) {
                    data.tests.forEach(test => {
                        html += `📡 ${test.name}\n`;
                        if (test.error) {
                            html += `   ❌ Erro: ${test.error}\n`;
                        } else {
                            html += `   Status: ${test.status} ${test.ok ? '✅' : '❌'}\n`;
                            html += `   Resposta: ${test.body}\n`;
                        }
                        html += '\n';
                    });
                }
                
                contentDiv.textContent = html;
                
                // Check if any test succeeded
                const successTest = data.tests?.find(t => t.ok);
                if (successTest) {
                    showToast('✅ API Funcionando', `${successTest.name} retornou sucesso!`, 'success');
                } else {
                    showToast('⚠️ API Indisponível', 'Nenhum endpoint da Monetizze respondeu. Verifique os detalhes.', 'warning');
                }
                
            } catch (error) {
                console.error('API test error:', error);
                showToast('❌ Erro', `Falha ao testar API: ${error.message}`, 'error');
            }
        }
        
        // Sync Monetizze sales
        async function syncMonetizze() {
            const startDate = document.getElementById('syncStartDate').value;
            const endDate = document.getElementById('syncEndDate').value;
            
            if (!startDate || !endDate) {
                showToast('⚠️ Atenção', 'Selecione as datas de início e fim', 'warning');
                return;
            }
            
            try {
                showToast('🔄 Sincronizando...', 'Buscando vendas da Monetizze', 'success');
                
                const response = await fetch(`${API_URL}/api/admin/sync-monetizze`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': `Bearer ${authToken}`
                    },
                    body: JSON.stringify({ startDate, endDate })
                });
                
                const data = await response.json();
                const resultDiv = document.getElementById('syncMonetizzeResult');
                const contentPre = document.getElementById('syncMonetizzeContent');
                
                // Check if it's an auth error (invalid admin token)
                if (response.status === 401 && data.error === 'Invalid or expired token') {
                    logout();
                    return;
                }
                
                // Handle other errors (like missing Monetizze credentials)
                if (!response.ok || !data.success) {
                    let errorMsg = `❌ Erro na sincronização\n\n`;
                    errorMsg += `Status: ${response.status}\n`;
                    errorMsg += `Erro: ${data.error || 'Desconhecido'}\n\n`;
                    
                    if (data.message) {
                        errorMsg += `Detalhes:\n${data.message}\n\n`;
                    }
                    
                    if (data.error === 'Monetizze API credentials not configured') {
                        errorMsg += `⚠️ Configure a variável MONETIZZE_CONSUMER_KEY no Railway:\n`;
                        errorMsg += `1. Acesse Railway → Seu Projeto → Variables\n`;
                        errorMsg += `2. Adicione: MONETIZZE_CONSUMER_KEY = sua chave da Monetizze\n`;
                        errorMsg += `3. Aguarde o deploy e tente novamente`;
                    }
                    
                    contentPre.textContent = errorMsg;
                    resultDiv.style.display = 'block';
                    showToast('❌ Erro', data.error || 'Falha na sincronização', 'error');
                    return;
                }
                
                // Success
                let output = `✅ Sincronização concluída!\n\n`;
                output += `📊 Total de vendas: ${data.total}\n`;
                output += `✅ Sincronizadas: ${data.synced}\n`;
                output += `⏭️ Ignoradas: ${data.skipped}\n`;
                
                if (data.errors && data.errors.length > 0) {
                    output += `\n⚠️ Erros (${data.errors.length}):\n`;
                    data.errors.forEach(err => {
                        output += `  - Venda ${err.sale}: ${err.error}\n`;
                    });
                }
                
                contentPre.textContent = output;
                resultDiv.style.display = 'block';
                
                showToast('✅ Sucesso!', `${data.synced} vendas sincronizadas`, 'success');
                
                // Reload data
                setTimeout(() => {
                    loadAllData();
                }, 2000);
                
            } catch (error) {
                console.error('Sync error:', error);
                const resultDiv = document.getElementById('syncMonetizzeResult');
                const contentPre = document.getElementById('syncMonetizzeContent');
                contentPre.textContent = `❌ Erro de conexão:\n\n${error.message}`;
                resultDiv.style.display = 'block';
                showToast('❌ Erro', 'Falha ao conectar com o servidor', 'error');
            }
        }
        
        // Helper: get local date string (YYYY-MM-DD) without UTC offset issues
        function getLocalDateString(date) {
            const d = date || new Date();
            const year = d.getFullYear();
            const month = String(d.getMonth() + 1).padStart(2, '0');
            const day = String(d.getDate()).padStart(2, '0');
            return `${year}-${month}-${day}`;
        }
        
        function syncMonetizzeToday() {
            const today = getLocalDateString();
            document.getElementById('syncStartDate').value = today;
            document.getElementById('syncEndDate').value = today;
            syncMonetizze();
        }
        
        function syncMonetizzeLast7Days() {
            const today = new Date();
            const sevenDaysAgo = new Date(today);
            sevenDaysAgo.setDate(today.getDate() - 7);
            
            document.getElementById('syncStartDate').value = getLocalDateString(sevenDaysAgo);
            document.getElementById('syncEndDate').value = getLocalDateString(today);
            syncMonetizze();
        }
        
        function saveWebhook() {
            const url = document.getElementById('webhookUrl').value;
            localStorage.setItem('webhookUrl', url);
            showToast('Salvo!', 'Webhook configurado', 'success');
        }
        
        // Security verification for dangerous actions
        async function verifySecurityPassword(actionName) {
            const password = prompt(`🔐 VERIFICAÇÃO DE SEGURANÇA\n\nAção: ${actionName}\n\nDigite sua senha de admin para confirmar:`);
            
            if (!password) {
                showToast('❌ Cancelado', 'Ação cancelada pelo usuário', 'warning');
                return false;
            }
            
            try {
                // Verify password with server
                const response = await fetch(`${API_URL}/api/admin/verify-password`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': `Bearer ${authToken}`
                    },
                    body: JSON.stringify({ password })
                });
                
                const data = await response.json();
                
                if (data.valid) {
                    return true;
                } else {
                    showToast('❌ Senha Incorreta', 'A senha digitada está incorreta', 'error');
                    return false;
                }
            } catch (error) {
                console.error('Password verification error:', error);
                showToast('❌ Erro', 'Falha ao verificar senha', 'error');
                return false;
            }
        }
        
        async function cleanTestTransactions() {
            if (!confirm('🧹 Limpar Transações de Teste?\n\nIsso vai remover todas as transações que contêm:\n- "TEST" no ID\n- "test" no email\n- "TEST" no produto\n\nDeseja continuar?')) {
                return;
            }
            
            // Require password verification
            const verified = await verifySecurityPassword('Limpar Transações de Teste');
            if (!verified) return;
            
            try {
                const response = await fetch(`${API_URL}/api/admin/transactions/test`, {
                    method: 'DELETE',
                    headers: { 'Authorization': `Bearer ${authToken}` }
                });
                
                if (response.status === 401 || response.status === 403) { logout(); return; }
                
                const data = await response.json();
                
                if (data.success) {
                    showToast('✅ Limpo!', `${data.deleted} transações de teste removidas`, 'success');
                    // Reload data
                    setTimeout(() => {
                        loadAllData();
                    }, 1000);
                } else {
                    showToast('❌ Erro', data.error || 'Falha ao limpar', 'error');
                }
            } catch (error) {
                console.error('Clean test error:', error);
                showToast('❌ Erro', 'Falha ao limpar transações de teste', 'error');
            }
        }
        
        async function migrateFunnelSource() {
            try {
                showToast('🔄 Migrando...', 'Classificando transações por origem...', 'info');
                
                const response = await fetch(`${API_URL}/api/admin/migrate-funnel-source`, {
                    method: 'POST',
                    headers: { 
                        'Authorization': `Bearer ${authToken}`,
                        'Content-Type': 'application/json'
                    }
                });
                
                if (response.status === 401 || response.status === 403) { logout(); return; }
                
                const data = await response.json();
                
                if (data.success) {
                    showToast('Migrado!', `${data.updated} transações classificadas como afiliado, ${data.nullFixed} definidas como principal.`, 'success');
                    loadSalesData();
                } else {
                    showToast('Erro', data.error || 'Falha na migração', 'error');
                }
            } catch (error) {
                console.error('Migration error:', error);
                showToast('Erro', 'Falha ao migrar transações', 'error');
            }
        }
        
        async function resetAndResyncTransactions() {
            if (!confirm('🔄 Resetar e Re-sincronizar Transações?\n\nIsso vai:\n1. DELETAR todas as transações existentes\n2. Sincronizar novamente da Monetizze (últimos 30 dias)\n3. As datas serão corrigidas\n\nDeseja continuar?')) {
                return;
            }
            
            // Require password verification
            const verified = await verifySecurityPassword('Resetar e Re-sincronizar Transações');
            if (!verified) return;
            
            try {
                showToast('🗑️ Deletando...', 'Removendo transações antigas', 'warning');
                
                // Step 1: Delete all transactions
                const deleteResponse = await fetch(`${API_URL}/api/admin/transactions/all`, {
                    method: 'DELETE',
                    headers: { 'Authorization': `Bearer ${authToken}` }
                });
                
                if (deleteResponse.status === 401 || deleteResponse.status === 403) { logout(); return; }
                
                const deleteData = await deleteResponse.json();
                
                if (!deleteData.success) {
                    showToast('❌ Erro', deleteData.error || 'Falha ao deletar', 'error');
                    return;
                }
                
                showToast('✅ Deletado!', `${deleteData.deleted} transações removidas`, 'success');
                
                // Step 2: Sync from Monetizze (last 30 days)
                showToast('🔄 Sincronizando...', 'Buscando vendas da Monetizze (últimos 30 dias)', 'success');
                
                const today = new Date();
                const thirtyDaysAgo = new Date(today);
                thirtyDaysAgo.setDate(today.getDate() - 30);
                
                const startDate = thirtyDaysAgo.toISOString().split('T')[0];
                const endDate = today.toISOString().split('T')[0];
                
                const syncResponse = await fetch(`${API_URL}/api/admin/sync-monetizze`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': `Bearer ${authToken}`
                    },
                    body: JSON.stringify({ startDate, endDate })
                });
                
                const syncData = await syncResponse.json();
                
                if (syncData.success) {
                    showToast('✅ Concluído!', `${syncData.synced} vendas sincronizadas com datas corretas!`, 'success');
                    // Reload data
                    setTimeout(() => {
                        loadAllData();
                    }, 2000);
                } else {
                    showToast('⚠️ Aviso', syncData.error || 'Sincronização pode ter falhado', 'warning');
                }
                
            } catch (error) {
                console.error('Reset and resync error:', error);
                showToast('❌ Erro', 'Falha no processo: ' + error.message, 'error');
            }
        }
        
        async function clearAllData() {
            if (!confirm('⚠️ ATENÇÃO!\n\nIsso vai DELETAR PERMANENTEMENTE:\n- Todos os leads\n- Todos os eventos do funil\n- Todas as transações\n- Todos os pedidos de reembolso\n\nTem certeza?')) {
                return;
            }
            
            // Require password verification
            const verified = await verifySecurityPassword('LIMPAR TODOS OS DADOS');
            if (!verified) return;
            
            try {
                const response = await fetch(`${API_URL}/api/admin/clear-all-data?confirm=yes-delete-everything`, {
                    method: 'DELETE',
                    headers: { 'Authorization': `Bearer ${authToken}` }
                });
                
                const data = await response.json();
                
                if (data.success) {
                    showToast('Dados Limpos!', `Removidos: ${data.deleted.leads} leads, ${data.deleted.funnel_events} eventos, ${data.deleted.transactions} transações`, 'success');
                    // Reload all data
                    setTimeout(() => {
                        loadAllData();
                    }, 500);
                } else {
                    showToast('Erro', data.error || 'Falha ao limpar dados', 'error');
                }
            } catch (error) {
                console.error('Error clearing data:', error);
                showToast('Erro', 'Falha ao limpar dados', 'error');
            }
        }
        
        function setChartPeriod(days) {
            document.querySelectorAll('.period-btn').forEach(b => b.classList.remove('active'));
            event.target.classList.add('active');
            showToast('Carregando...', `Mostrando últimos ${days} dias`, 'success');
        }
        
        // ==================== REAL-TIME DASHBOARD ====================
        let autoRefreshInterval = null;
        let lastSalesCount = 0;
        let lastLeadsCount = 0;
        let isAutoRefreshEnabled = true;
        
        // Request notification permission on load
        if ('Notification' in window && Notification.permission === 'default') {
            Notification.requestPermission();
        }
        
        function startAutoRefresh() {
            if (autoRefreshInterval) clearInterval(autoRefreshInterval);
            
            // Refresh every 30 seconds
            autoRefreshInterval = setInterval(async () => {
                if (!isAutoRefreshEnabled || document.hidden) return;
                
                try {
                    const [statsRes, salesRes] = await Promise.all([
                        fetch(`${API_URL}/api/admin/stats`, { headers: { 'Authorization': `Bearer ${authToken}` } }),
                        fetch(`${API_URL}/api/admin/sales`, { headers: { 'Authorization': `Bearer ${authToken}` } })
                    ]);
                    
                    if (statsRes.ok && salesRes.ok) {
                        const stats = await statsRes.json();
                        const sales = await salesRes.json();
                        
                        // Check for new sales
                        if (lastSalesCount > 0 && sales.approved > lastSalesCount) {
                            const newSales = sales.approved - lastSalesCount;
                            showNewSaleNotification(newSales, sales.revenue);
                        }
                        
                        // Check for new leads
                        if (lastLeadsCount > 0 && stats.total > lastLeadsCount) {
                            const newLeads = stats.total - lastLeadsCount;
                            showToast('🆕 Novo Lead!', `${newLeads} novo(s) lead(s) capturado(s)`, 'success');
                        }
                        
                        lastSalesCount = sales.approved || 0;
                        lastLeadsCount = stats.total || 0;
                        
                        // Update real-time indicator
                        updateRealtimeIndicator(true);
                        
                        // Silently update data
                        loadAllData();
                    }
                } catch (error) {
                    updateRealtimeIndicator(false);
                }
            }, 30000); // 30 seconds
            
            // Initial count
            initializeCounts();
        }
        
        async function initializeCounts() {
            try {
                const [statsRes, salesRes] = await Promise.all([
                    fetch(`${API_URL}/api/admin/stats`, { headers: { 'Authorization': `Bearer ${authToken}` } }),
                    fetch(`${API_URL}/api/admin/sales`, { headers: { 'Authorization': `Bearer ${authToken}` } })
                ]);
                
                if (statsRes.ok) {
                    const stats = await statsRes.json();
                    lastLeadsCount = stats.total || 0;
                }
                if (salesRes.ok) {
                    const sales = await salesRes.json();
                    lastSalesCount = sales.approved || 0;
                }
            } catch (e) { console.log('Init counts error:', e); }
        }
        
        function showNewSaleNotification(count, totalRevenue) {
            // Toast notification
            showToast('💰 Nova Venda!', `${count} nova(s) venda(s) - Total: R$ ${totalRevenue.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}`, 'success');
            
            // Browser notification
            if ('Notification' in window && Notification.permission === 'granted') {
                new Notification('💰 Nova Venda no ZapSpy!', {
                    body: `${count} nova(s) venda(s) aprovada(s)!`,
                    icon: '💰',
                    tag: 'new-sale'
                });
            }
            
            // Play sound (optional)
            try {
                const audio = new Audio('data:audio/wav;base64,UklGRnoGAABXQVZFZm10IBAAAAABAAEAQB8AAEAfAAABAAgAZGF0YQoGAACBhYqFbF1oa2d0aHNlj5qUqZ6Ws6acoq+el6CmpZmhsKiirqyqoa2npKSop6aprqqmqa6rp6uwrKmtsKyprLGsqq2xrKqtsayqrbGsqq2xrKqtsayqrbCsqa2wrKmtr6yprK+sqaywrKmsr6yprK+sqaywrKmsr6yprK+sqayvq6msr6yprK6sqKuurKitrqyora2sqK2trKitrKynrKysqKysrKesq6ynq6usqKurrKesq6ynq6qsp6uqrKerqqynq6qsp6uqrKerqqunqqmrp6qpq6eqqaunqamrp6mpq6eoqaunqKirp6ioq6enqKunp6irp6eoq6eoqKunqKirp6ioq6eoqKunqKirp6ioq6eoqKunqKirp6ioq6eoqKunqKirp6ioq6eoqKunqKirp6ioq6eoqKunqKirp6ioq6enqKunp6irp6eoq6enqKunp6erp6enq6enp6unp6erp6enq6enp6unp6erp6enq6enp6unp6erp6enq6enp6unp6erp6enq6enp6unp6erp6enq6enp6unp6erp6enq6enp6unp6erp6enq6enp6unp6erp6enq6enp6unp6erp6enq6enp6unp6erp6enq6enp6unp6erp6enq6enp6unp6erp6enq6enp6unp6erp6enq6enp6unp6erp6enqw==');
                audio.volume = 0.3;
                audio.play().catch(() => {});
            } catch (e) {}
        }
        
        function updateRealtimeIndicator(connected) {
            const indicator = document.getElementById('realtimeIndicator');
            if (indicator) {
                indicator.innerHTML = connected 
                    ? '<span style="display: inline-flex; align-items: center; gap: 6px; padding: 6px 12px; background: rgba(16, 185, 129, 0.15); border: 1px solid rgba(16, 185, 129, 0.3); border-radius: 20px; font-size: 11px; font-weight: 600; color: #10b981;"><span style="width: 8px; height: 8px; background: #10b981; border-radius: 50%; animation: pulse 2s infinite;"></span>Ao vivo</span>'
                    : '<span style="display: inline-flex; align-items: center; gap: 6px; padding: 6px 12px; background: rgba(239, 68, 68, 0.15); border: 1px solid rgba(239, 68, 68, 0.3); border-radius: 20px; font-size: 11px; font-weight: 600; color: #ef4444;"><span style="width: 8px; height: 8px; background: #ef4444; border-radius: 50%;"></span>Offline</span>';
            }
        }
        
        function toggleAutoRefresh() {
            isAutoRefreshEnabled = !isAutoRefreshEnabled;
            const btn = document.getElementById('autoRefreshBtn');
            if (btn) {
                btn.innerHTML = isAutoRefreshEnabled ? '⏸️' : '▶️';
                btn.title = isAutoRefreshEnabled ? 'Pausar atualização automática' : 'Retomar atualização automática';
            }
            showToast(isAutoRefreshEnabled ? 'Auto-refresh ativado' : 'Auto-refresh pausado', '', 'success');
        }
        
        // Start auto-refresh when dashboard loads
        if (authToken) {
            startAutoRefresh();
        }
        
        // ==================== ROI CALCULATOR ====================
        let roiModalVisible = false;
        
        function openROICalculator() {
            document.getElementById('roiCalculatorModal').classList.add('active');
            roiModalVisible = true;
            calculateROI();
        }
        
        function closeROICalculator() {
            document.getElementById('roiCalculatorModal').classList.remove('active');
            roiModalVisible = false;
        }
        
        function calculateROI() {
            const adSpend = parseFloat(document.getElementById('roiAdSpend').value) || 0;
            const revenue = parseFloat(document.getElementById('roiRevenue').value) || 0;
            const leads = parseInt(document.getElementById('roiLeads').value) || 0;
            const sales = parseInt(document.getElementById('roiSales').value) || 0;
            
            // Calculations
            const profit = revenue - adSpend;
            const roi = adSpend > 0 ? ((profit / adSpend) * 100) : 0;
            const cpl = leads > 0 ? (adSpend / leads) : 0; // Cost per lead
            const cpa = sales > 0 ? (adSpend / sales) : 0; // Cost per acquisition
            const convRate = leads > 0 ? ((sales / leads) * 100) : 0;
            const avgTicket = sales > 0 ? (revenue / sales) : 0;
            const roas = adSpend > 0 ? (revenue / adSpend) : 0; // Return on ad spend
            
            // Update display
            document.getElementById('roiProfit').textContent = 'R$ ' + profit.toLocaleString('pt-BR', { minimumFractionDigits: 2 });
            document.getElementById('roiProfit').style.color = profit >= 0 ? '#10b981' : '#ef4444';
            
            document.getElementById('roiPercentage').textContent = roi.toFixed(1) + '%';
            document.getElementById('roiPercentage').style.color = roi >= 0 ? '#10b981' : '#ef4444';
            
            document.getElementById('roiCPL').textContent = 'R$ ' + cpl.toLocaleString('pt-BR', { minimumFractionDigits: 2 });
            document.getElementById('roiCPA').textContent = 'R$ ' + cpa.toLocaleString('pt-BR', { minimumFractionDigits: 2 });
            document.getElementById('roiConvRate').textContent = convRate.toFixed(1) + '%';
            document.getElementById('roiAvgTicket').textContent = 'R$ ' + avgTicket.toLocaleString('pt-BR', { minimumFractionDigits: 2 });
            document.getElementById('roiROAS').textContent = roas.toFixed(2) + 'x';
            document.getElementById('roiROAS').style.color = roas >= 1 ? '#10b981' : '#ef4444';
        }
        
        async function loadROIFromData() {
            try {
                const [statsRes, salesRes] = await Promise.all([
                    fetch(`${API_URL}/api/admin/stats`, { headers: { 'Authorization': `Bearer ${authToken}` } }),
                    fetch(`${API_URL}/api/admin/sales`, { headers: { 'Authorization': `Bearer ${authToken}` } })
                ]);
                
                if (statsRes.ok && salesRes.ok) {
                    const stats = await statsRes.json();
                    const sales = await salesRes.json();
                    
                    document.getElementById('roiLeads').value = stats.total || 0;
                    document.getElementById('roiSales').value = sales.approved || 0;
                    document.getElementById('roiRevenue').value = (sales.revenue || 0).toFixed(2);
                    
                    calculateROI();
                    showToast('Dados carregados', 'Valores atualizados do painel', 'success');
                }
            } catch (e) {
                showToast('Erro', 'Não foi possível carregar dados', 'error');
            }
        }
        
        // ==================== PERIOD COMPARISON ====================
        async function loadPeriodComparison() {
            try {
                const response = await fetch(`${API_URL}/api/admin/stats/comparison`, { 
                    headers: { 'Authorization': `Bearer ${authToken}` } 
                });
                
                if (!response.ok) return;
                
                const data = await response.json();
                
                if (data.currentWeek && data.previousWeek) {
                    // Update comparison cards
                    updateComparisonCard('leadsComparison', data.currentWeek.leads, data.previousWeek.leads, 'leads');
                    updateComparisonCard('salesComparison', data.currentWeek.sales, data.previousWeek.sales, 'vendas');
                    updateComparisonCard('revenueComparison', data.currentWeek.revenue, data.previousWeek.revenue, 'faturamento', true);
                }
            } catch (e) {
                console.log('Period comparison not available');
            }
        }
        
        function updateComparisonCard(elementId, current, previous, label, isCurrency = false) {
            const el = document.getElementById(elementId);
            if (!el) return;
            
            const change = previous > 0 ? ((current - previous) / previous * 100) : (current > 0 ? 100 : 0);
            const isPositive = change >= 0;
            const arrow = isPositive ? '↑' : '↓';
            const color = isPositive ? '#10b981' : '#ef4444';
            
            const currentDisplay = isCurrency 
                ? 'R$ ' + current.toLocaleString('pt-BR', { minimumFractionDigits: 2 })
                : current.toLocaleString('pt-BR');
            
            el.innerHTML = `
                <div style="font-size: 12px; color: rgba(255,255,255,0.5); margin-bottom: 4px;">${label}</div>
                <div style="font-size: 24px; font-weight: 700; color: #fff;">${currentDisplay}</div>
                <div style="display: flex; align-items: center; gap: 4px; margin-top: 4px;">
                    <span style="color: ${color}; font-weight: 600; font-size: 13px;">${arrow} ${Math.abs(change).toFixed(1)}%</span>
                    <span style="color: rgba(255,255,255,0.4); font-size: 11px;">vs semana anterior</span>
                </div>
            `;
        }
        
        // ==================== HEATMAP ====================
        async function loadHeatmapData() {
            try {
                const response = await fetch(`${API_URL}/api/admin/stats/comparison`, {
                    headers: { 'Authorization': `Bearer ${authToken}` }
                });
                
                if (!response.ok) return;
                
                const data = await response.json();
                
                // Update comparison cards
                if (data.currentWeek && data.previousWeek) {
                    updateComparisonCard('leadsComparison', data.currentWeek.leads, data.previousWeek.leads, 'leads');
                    updateComparisonCard('salesComparison', data.currentWeek.sales, data.previousWeek.sales, 'vendas');
                    updateComparisonCard('revenueComparison', data.currentWeek.revenue, data.previousWeek.revenue, 'faturamento', true);
                }
                
                // Render heatmap
                if (data.hourlyHeatmap) {
                    renderHeatmap(data.hourlyHeatmap);
                }
            } catch (e) {
                console.log('Heatmap data not available:', e);
            }
        }
        
        function renderHeatmap(hourlyData) {
            const grid = document.getElementById('heatmapGrid');
            if (!grid) return;
            
            const dayNames = ['Dom', 'Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb'];
            
            // Create a 7x24 matrix
            const matrix = {};
            let maxCount = 1;
            
            hourlyData.forEach(item => {
                const key = `${item.day_of_week}-${item.hour}`;
                matrix[key] = parseInt(item.count);
                if (matrix[key] > maxCount) maxCount = matrix[key];
            });
            
            let html = '';
            
            for (let day = 0; day < 7; day++) {
                // Day label
                html += `<div style="display: flex; align-items: center; justify-content: flex-end; padding-right: 8px; font-weight: 600; color: var(--text-secondary); font-size: 11px;">${dayNames[day]}</div>`;
                
                // Hours
                for (let hour = 0; hour < 24; hour++) {
                    const count = matrix[`${day}-${hour}`] || 0;
                    const intensity = maxCount > 0 ? (count / maxCount) : 0;
                    const opacity = 0.1 + (intensity * 0.9);
                    const bgColor = `rgba(16, 185, 129, ${opacity})`;
                    
                    html += `
                        <div style="
                            height: 28px;
                            background: ${count > 0 ? bgColor : 'rgba(255,255,255,0.03)'};
                            border-radius: 4px;
                            display: flex;
                            align-items: center;
                            justify-content: center;
                            font-size: 9px;
                            color: ${intensity > 0.5 ? '#fff' : 'var(--text-muted)'};
                            cursor: pointer;
                            transition: transform 0.15s;
                        " 
                        onmouseover="this.style.transform='scale(1.1)'; this.style.zIndex='10';"
                        onmouseout="this.style.transform='scale(1)'; this.style.zIndex='1';"
                        title="${dayNames[day]} às ${hour}:00 - ${count} leads"
                        >${count > 0 ? count : ''}</div>
                    `;
                }
            }
            
            grid.innerHTML = html;
        }
        
        // Load heatmap on page load
        document.addEventListener('DOMContentLoaded', () => {
            if (authToken) {
                setTimeout(loadHeatmapData, 1000);
            }
        });
        
        // ==================== SPARKLINES ====================
        function createSparkline(data, containerId) {
            const container = document.getElementById(containerId);
            if (!container || !data || data.length === 0) return;
            
            const maxValue = Math.max(...data, 1);
            const bars = data.map(value => {
                const height = Math.max((value / maxValue) * 30, 2);
                return `<div class="sparkline-bar" style="height: ${height}px;" title="${value}"></div>`;
            }).join('');
            
            container.innerHTML = `<div class="sparkline">${bars}</div>`;
        }
        
        // ==================== SKELETON LOADERS ====================
        function showSkeletonLoading(elementId, type = 'text') {
            const el = document.getElementById(elementId);
            if (!el) return;
            
            if (type === 'card') {
                el.innerHTML = '<div class="skeleton-loader skeleton-card"></div>';
            } else if (type === 'chart') {
                el.innerHTML = '<div class="skeleton-loader skeleton-chart"></div>';
            } else if (type === 'table') {
                el.innerHTML = `
                    <tr><td colspan="10" style="padding: 16px;">
                        <div class="skeleton-loader skeleton-text full"></div>
                        <div class="skeleton-loader skeleton-text" style="margin-top: 12px;"></div>
                        <div class="skeleton-loader skeleton-text short" style="margin-top: 12px;"></div>
                    </td></tr>
                `;
            } else {
                el.innerHTML = '<div class="skeleton-loader skeleton-text"></div>';
            }
        }
        
        function animateValue(elementId, newValue, isCurrency = false, duration = 500) {
            const el = document.getElementById(elementId);
            if (!el) return;
            
            const startValue = parseFloat(el.textContent.replace(/[^\d.-]/g, '')) || 0;
            const endValue = parseFloat(newValue) || 0;
            const startTime = performance.now();
            
            function update(currentTime) {
                const elapsed = currentTime - startTime;
                const progress = Math.min(elapsed / duration, 1);
                
                // Easing function (ease-out)
                const easeOut = 1 - Math.pow(1 - progress, 3);
                const currentValue = startValue + (endValue - startValue) * easeOut;
                
                if (isCurrency) {
                    el.textContent = 'R$ ' + currentValue.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                } else if (Number.isInteger(endValue)) {
                    el.textContent = Math.round(currentValue).toLocaleString('pt-BR');
                } else {
                    el.textContent = currentValue.toFixed(1) + '%';
                }
                
                if (progress < 1) {
                    requestAnimationFrame(update);
                }
            }
            
            el.classList.add('updating');
            requestAnimationFrame(update);
            
            setTimeout(() => el.classList.remove('updating'), duration);
        }
        
        // ==================== VISUAL FUNNEL ====================
        function renderVisualFunnel(stats) {
            const container = document.getElementById('visualFunnelContainer');
            if (!container) return;
            
            const stages = [
                { key: 'page_view_landing', label: 'Landing', icon: '🏠', color: '#6366f1' },
                { key: 'page_view_phone', label: 'Telefone', icon: '📱', color: '#8b5cf6' },
                { key: 'phone_submitted', label: 'Tel. Enviado', icon: '✅', color: '#a855f7' },
                { key: 'page_view_cta', label: 'CTA', icon: '🎯', color: '#ec4899' },
                { key: 'email_captured', label: 'Email', icon: '📧', color: '#f43f5e' },
                { key: 'checkout_clicked', label: 'Checkout', icon: '🛒', color: '#10b981' }
            ];
            
            const statsMap = {};
            (stats || []).forEach(s => statsMap[s.event] = parseInt(s.unique_visitors));
            
            const maxValue = Math.max(...stages.map(s => statsMap[s.key] || 0), 1);
            let previousValue = null;
            
            let html = '<div style="display: flex; flex-direction: column; gap: 8px;">';
            
            stages.forEach((stage, index) => {
                const value = statsMap[stage.key] || 0;
                const width = (value / maxValue) * 100;
                const dropRate = previousValue !== null && previousValue > 0 
                    ? Math.round((1 - value / previousValue) * 100) 
                    : 0;
                
                html += `
                    <div style="display: flex; align-items: center; gap: 16px;">
                        <div style="width: 100px; font-size: 12px; color: var(--text-muted); text-align: right; display: flex; align-items: center; justify-content: flex-end; gap: 6px;">
                            <span>${stage.icon}</span>
                            <span>${stage.label}</span>
                        </div>
                        <div style="flex: 1; position: relative;">
                            <div style="
                                height: 36px;
                                background: linear-gradient(90deg, ${stage.color}, ${stage.color}88);
                                width: ${Math.max(width, 5)}%;
                                border-radius: 6px;
                                display: flex;
                                align-items: center;
                                justify-content: flex-end;
                                padding-right: 12px;
                                transition: width 0.5s ease;
                                box-shadow: 0 2px 8px ${stage.color}40;
                            ">
                                <span style="font-weight: 700; color: #fff; font-size: 14px;">${value.toLocaleString('pt-BR')}</span>
                            </div>
                        </div>
                        ${dropRate > 0 ? `<div style="width: 60px; font-size: 12px; color: #ef4444; font-weight: 600;">-${dropRate}%</div>` : '<div style="width: 60px;"></div>'}
                    </div>
                `;
                
                previousValue = value;
            });
            
            html += '</div>';
            container.innerHTML = html;
        }

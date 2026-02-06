# Bugs e Melhorias – Funil ZapSpy.ai

## MELHORIAS APLICADAS (v2.1.0)

### 1. Redirecionamento CTA por gênero (`conversas.html`)
- **Problema:** Quem escolhia "Monitor Partner (Man)" era enviado para `cta-female.html` e vice-versa.
- **Correção:** Lógica corrigida: `gender=male` → `cta-male.html`, `gender=female` → `cta-female.html`.

### 2. Tratamento de número não cadastrado no WhatsApp (`phone.html`)
- **Problema:** Se Z-API retornava `exists: false`, o funil continuava sem avisar.
- **Correção:** Adicionada tela de erro "Not registered on WhatsApp" com botão "Try Another Number".

### 3. Formatação de telefone internacional (`phone.html`)
- **Problema:** Máscara BR-only (11 dígitos).
- **Correção:** Função `formatPhoneNumber()` com suporte a:
  - EUA/Canadá (+1): `(XXX) XXX-XXXX`
  - Brasil (+55): `(XX) XXXXX-XXXX` ou `(XX) XXXX-XXXX`
  - UK (+44): `XXXX XXX XXXX`
  - Genérico: espaços a cada 3 dígitos

### 4. Limite de dígitos do telefone (`phone.html`)
- **Problema:** Limitado a 11 dígitos.
- **Correção:** Aumentado para 15 dígitos.

### 5. Passagem de gender/phone entre páginas
- **Problema:** Chat não recebia gender/phone de conversas e não repassava para CTA.
- **Correção:** 
  - `conversas.html` → passa `gender` e `phone` na URL do `chat.html`
  - `chat.html` → lê `gender` e `phone` da URL e redireciona para CTA correta

### 6. Fallback de avatar no chat (`chat.html`)
- **Problema:** URL vazia ou inválida quebrava a imagem.
- **Correção:** Validação + `onerror` handler para usar `perfil-espionado.jpeg` como fallback.

### 7. Tradução do chat.html para inglês
- **Problema:** Chat estava em português enquanto o funil é em inglês.
- **Correção:** Todos os textos traduzidos:
  - "Contato" → "Contact"
  - "mensagens bloqueadas" → "locked messages"
  - "Desbloquear" → "Unlock"
  - "Ontem/Hoje" → "Yesterday/Today"
  - "PROVA ENCONTRADA" → "EVIDENCE FOUND"
  - "MENSAGEM DELETADA" → "DELETED MESSAGE"
  - "Foto/Vídeo/Localização bloqueada" → "Locked photo/video/location"
  - Mensagens do chat traduzidas

### 8. Remoção de variáveis RapidAPI não usadas (`phone.html`)
- **Problema:** `RAPIDAPI_KEY` e `RAPIDAPI_HOST` declarados mas nunca usados (verificação usa Z-API).
- **Correção:** Variáveis removidas.

### 9. Correção de CSS morto (`landing.html`)
- **Problema:** `.alert-badge` no media query mas HTML usa `.stats-badge`.
- **Correção:** Renomeado para `.stats-badge`.

### 10. Remoção de screenshot por visibilitychange (`cta*.html`)
- **Problema:** Trocar de aba disparava "SCREENSHOT DETECTED" (falso positivo).
- **Correção:** Listener de `visibilitychange` removido dos 3 arquivos CTA. A detecção por teclas (PrintScreen, etc.) permanece.

---

## ANÁLISE COMPLETA DO PROJETO (PÓS-MELHORIAS)

### Estrutura de Arquivos

| Arquivo | Função |
|---------|--------|
| `index.html` | Página inicial com escolha de gênero (Man/Woman) |
| `landing.html` | Landing page com benefícios e CTA "Spy Now" |
| `phone.html` | Captura de telefone + verificação Z-API + simulação de hack |
| `conversas.html` | Lista fake de conversas estilo WhatsApp iOS |
| `chat.html` | Chat individual com mensagens bloqueadas |
| `cta.html` | Página de vendas principal (sem gênero) |
| `cta-male.html` | Página de vendas para target masculino |
| `cta-female.html` | Página de vendas para target feminino |
| `imagens/` | Avatares, fotos blur, ícones |

### Fluxo Completo

```
index.html (escolha gênero)
    ↓
landing.html?gender=male|female
    ↓
phone.html?gender=... (input telefone)
    ↓
[Z-API verifica número]
    ↓
├─ Se não existe → Tela de erro "Not registered"
│                    ↓ Botão "Try Another Number"
│
└─ Se existe → Tela de confirmação (perfil + deleted count)
                 ↓
              Simulação terminal (60s)
                 ↓
              conversas.html?gender=...&phone=...
                 ↓
              ├─ Clica em conversa → chat.html?...&gender=...&phone=...
              │                         ↓
              │                      Clica "Unlock" → cta-male|female|.html?phone=...
              │
              └─ Clica "Unlock" → cta-male|female|.html?phone=...
                                    ↓
                                 Monetizze checkout
```

### Tecnologias e Integrações

| Tecnologia | Uso |
|------------|-----|
| **Z-API** | Verificação se número está no WhatsApp + foto de perfil |
| **ipapi.co** | Geolocalização por IP (landing) |
| **RapidAPI IP Geo** | Localização para mapa na CTA |
| **OpenStreetMap** | Embed de mapa na CTA |
| **Meta Pixel** | Tracking (ID: 726299943423075) |
| **Monetizze** | Checkout (KFS429964) |
| **Web Audio API** | Sons de notificação |
| **Vibration API** | Feedback tátil mobile |

### Recursos de Conversão

- Timer regressivo (8 min)
- "Only X slots left" com decremento
- Contador de "pessoas vendo agora"
- Notificações fake de compra com bandeiras de países
- "Suspicion Index" com barra de progresso
- "Most Frequent Contact" bloqueado
- Mensagem deletada com trechos censurados
- Exit intent popup
- Screenshot warning (por tecla)
- Prova social (reviews, avatares)

### Performance

- Efeito Matrix otimizado (requestAnimationFrame + pause em hidden)
- Limite de colunas (50-60)
- Frame rate reduzido (80-100ms)
- beforeunload cleanup

### Responsividade

- Media queries para 480px, 380px, 600px height
- Safe area para notch iOS
- Fontes responsivas com clamp()

---

## MELHORIAS FUTURAS SUGERIDAS

### 1. Unificar as 3 páginas CTA
- `cta.html`, `cta-male.html` e `cta-female.html` têm milhares de linhas duplicadas
- Sugestão: Uma única página que leia `gender` da URL e ajuste textos/imagens

### 2. Extrair sistemas de áudio/vibração
- `WhatsAppAudio` e `VibrationSystem` repetidos em phone, conversas, cta
- Sugestão: Criar `js/audio-vibration.js` compartilhado

### 3. Implementar páginas de Termos/Privacidade
- Links no footer vão para `#`
- Sugestão: Criar páginas reais ou modal com conteúdo

### 4. Mover credenciais para backend
- Z-API tokens, Pixel ID expostos no HTML
- Sugestão: Proxy backend para chamadas de API

### 5. Adicionar mais variação nos números
- "2,847 people", "127 purchases today" são fixos
- Sugestão: Pequena randomização periódica

---

## RESUMO DAS MUDANÇAS

| Tipo | Quantidade | Status |
|------|------------|--------|
| Bugs corrigidos | 10 | ✅ Completo |
| Copy traduzida | 1 arquivo | ✅ Completo |
| Código removido | 3 itens | ✅ Completo |
| Melhorias futuras | 5 sugestões | ⏳ Pendente |

**Versão atual: 2.1.0** (com todas as correções aplicadas)

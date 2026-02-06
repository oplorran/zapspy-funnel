# ZapSpy.ai - WhatsApp Stalker Funnel

Funil de vendas estilo "stalker" para WhatsApp, baseado no modelo do InstaStalker.

## Estrutura do Projeto

```
whatsapp_stalker_v2/
├── index.html        # Landing page + input + confirmação + processamento
├── conversas.html    # Lista de conversas fake do WhatsApp
├── chat-1.html       # Chat individual com mensagens bloqueadas
├── cta.html          # Página de vendas/checkout
├── imagens/          # Imagens e ícones
└── README.md         # Este arquivo
```

## Fluxo do Funil

1. **Landing Page** - Usuário vê headline e clica em "Spy Now"
2. **Input de Telefone** - Usuário insere número com máscara (+1)
3. **Verificação API** - Sistema verifica se número está no WhatsApp (real)
4. **Confirmação** - Mostra dados do perfil e pede confirmação
5. **Processamento** - Animação de "quebrando criptografia"
6. **Conversas** - Lista de conversas fake com notificações
7. **CTA/Vendas** - Página de checkout com timer e benefícios

## Integração com API (RapidAPI)

O projeto usa a API "WhatsApp Data" do RapidAPI para:
- Verificar se o número está registrado no WhatsApp
- Buscar foto de perfil (apenas para contas business)
- Buscar status/recado

### Configuração da API

No arquivo `index.html`, atualize as credenciais:

```javascript
const RAPIDAPI_KEY = 'SUA_API_KEY_AQUI';
const RAPIDAPI_HOST = 'whatsapp-data.p.rapidapi.com';
```

## Personalização

### Meta Pixel
Substitua `SEU_PIXEL_ID_AQUI` pelo seu ID do Meta Pixel em todos os arquivos HTML.

### URL de Checkout
Na função `handlePurchase()` do arquivo `cta.html`, altere a URL de redirecionamento:

```javascript
function handlePurchase() {
    window.location.href = 'SUA_URL_DE_CHECKOUT';
}
```

### Preços
Altere os valores de preço no arquivo `cta.html`:
- Preço original: `$49.99`
- Preço promocional: `$9.99`

### Cores
O projeto usa verde WhatsApp (#25D366). Para alterar, busque e substitua:
- `#25D366` - Verde principal
- `#128C7E` - Verde secundário
- `#111B21` - Fundo escuro
- `#0a0f0d` - Fundo mais escuro

## Hospedagem

O projeto é 100% estático (HTML/CSS/JS) e pode ser hospedado em:
- Netlify
- Vercel
- GitHub Pages
- Cloudflare Pages
- Qualquer servidor web

## Notas Importantes

1. **Efeito Matrix**: Foi otimizado para não travar navegadores. O intervalo é de 50ms e para quando a aba não está visível.

2. **API WhatsApp**: A foto de perfil só está disponível para contas business. Para contas pessoais, é exibido um avatar genérico.

3. **Validação**: O sistema valida se o número está registrado no WhatsApp antes de prosseguir. Se não estiver, mostra erro.

## Changelog

### v2.0.0
- Corrigido bug do efeito Matrix que travava navegadores
- Integração com API do WhatsApp para validação real
- Otimização de performance
- Limpeza de recursos antes de redirecionamentos

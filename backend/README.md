# ZapSpy.ai Backend

Backend API para captura de leads e painel administrativo.

## 🚀 Deploy no Railway

### Passo 1: Criar conta no Railway
1. Acesse [railway.app](https://railway.app)
2. Faça login com sua conta GitHub

### Passo 2: Criar banco de dados PostgreSQL
1. Clique em **"New Project"**
2. Selecione **"Provision PostgreSQL"**
3. Aguarde a criação do banco
4. Clique no banco criado e vá em **"Variables"**
5. Copie o valor de `DATABASE_URL`

### Passo 3: Criar o serviço do Backend
1. No mesmo projeto, clique em **"New"** → **"GitHub Repo"**
2. Selecione o repositório do seu funil
3. Configure o **Root Directory** como `backend`
4. Adicione as variáveis de ambiente:

```env
DATABASE_URL=postgresql://... (copie do PostgreSQL)
JWT_SECRET=sua-chave-secreta-muito-forte-aqui
ADMIN_EMAIL=seu-email@exemplo.com
ADMIN_PASSWORD=sua-senha-forte
FRONTEND_URL=https://seu-dominio.com
PORT=3000
```

### Passo 4: Executar migração do banco
Após o deploy, abra o terminal do Railway e execute:
```bash
npm run db:migrate
```

### Passo 5: Acessar o painel
Sua URL será algo como: `https://seu-projeto.up.railway.app/admin`

---

## 📁 Estrutura

```
backend/
├── server.js          # API Express
├── migrate.js         # Script de migração do banco
├── package.json       # Dependências
├── .env.example       # Exemplo de variáveis
├── public/
│   └── admin.html     # Painel administrativo
└── README.md          # Este arquivo
```

---

## 🔗 Endpoints da API

### Públicos
- `POST /api/leads` - Capturar novo lead
- `GET /api/health` - Health check

### Protegidos (requer JWT)
- `POST /api/admin/login` - Login do admin
- `GET /api/admin/leads` - Listar leads
- `GET /api/admin/stats` - Estatísticas
- `PUT /api/admin/leads/:id` - Atualizar lead
- `DELETE /api/admin/leads/:id` - Deletar lead
- `GET /api/admin/leads/export` - Exportar CSV

---

## 🔧 Desenvolvimento Local

1. Clone o repositório
2. Copie `.env.example` para `.env` e configure
3. Instale dependências:
```bash
cd backend
npm install
```

4. Execute a migração:
```bash
npm run db:migrate
```

5. Inicie o servidor:
```bash
npm run dev
```

6. Acesse: `http://localhost:3000/admin`

---

## 📊 Configurar o Frontend

Após o deploy, atualize o arquivo `js/email-capture.js` com a URL do seu backend:

```javascript
const WEBHOOK_URL = 'https://seu-projeto.up.railway.app/api/leads';
```

---

## 🔒 Segurança

- Troque SEMPRE as credenciais padrão
- Use uma `JWT_SECRET` forte (mínimo 32 caracteres)
- Configure `FRONTEND_URL` para restringir CORS
- Habilite HTTPS (Railway faz isso automaticamente)

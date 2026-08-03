# Judge Dashboard

裁判机前端大屏项目，基于 `React + Vite + TypeScript`。

## 开发联调

默认开发模式：

- 前端开发服务器：`http://127.0.0.1:5173`
- 裁判后端默认代理目标：`http://127.0.0.1:18080`

启动前请先确认 JudgeWeb 后端已经运行。

### 1. 使用默认本机联调

不需要额外配置，直接运行：

```bash
npm install
npm run dev
```

此时：

- `/api/*` 会被 Vite 自动代理到 `http://127.0.0.1:18080`
- `WS /api/v1/ws/live` 也会走同源地址

### 2. 联调到其他裁判机地址

复制 `.env.example` 为 `.env.local`，按需填写：

```env
VITE_DEV_PROXY_TARGET=http://192.168.1.10:18080
VITE_API_BASE_URL=
VITE_WS_BASE_URL=
```

说明：

- `VITE_DEV_PROXY_TARGET` 用于 `vite dev` 的代理目标
- 如果前端和后端不走同源，也可以直接填写：
  - `VITE_API_BASE_URL=http://192.168.1.10:18080/api/v1`
  - `VITE_WS_BASE_URL=ws://192.168.1.10:18080/api/v1/ws/live`

## 生产构建

```bash
npm run build
```

构建后产物输出到 `dist/`。

## 当前使用的后端接口

- `GET /api/v1/match/overview`
- `GET /api/v1/match/current`
- `GET /api/v1/match/teams`
- `GET /api/v1/match/scoreboard`
- `GET /api/v1/system/components`
- `GET /api/v1/recovery/status`
- `WS /api/v1/ws/live`

## 联调注意事项

- 开发态优先依赖 Vite 代理，不要手动把前端代码里的 `/api/v1` 改成固定 IP
- 如果页面显示“离线 - 降级轮询中”，先检查 JudgeWeb 是否正常运行
- 如果页面无数据，先在浏览器直接访问 `http://裁判机IP:18080/api/v1/match/overview`

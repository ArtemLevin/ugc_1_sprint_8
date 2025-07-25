"""
Точка входа в приложение User Action Collector.
Поддерживает:
- Запуск через Hypercorn/ASGI (продакшен)
- Локальную разработку с Uvicorn reload
- Асинхронные обработчики и цепочку событий
"""

from app_v_1.factory import create_app

app = create_app()

if __name__ == "__main__":
    """
    Запуск сервера напрямую (не через WSGI/ASGI-сервер).
    Используется только при выполнении: python app_v_1/main.py
    """
    import uvicorn

    uvicorn.run(
        "app_v_1.main:app_v_1",
        host="127.0.0.1",
        port=5000,
        reload=True,
        log_level="info",
        use_colors=True,
        factory=False
    )

# Используем официальный образ Tarantool
FROM tarantool/tarantool:3

# Копируем файл конфигурации внутрь контейнера
COPY . /etc/tarantool/

# Устанавливаем переменные окружения для базы данных Tarantool
ENV TT_APP_NAME='keyvalue-emu-app'
ENV TT_MEMTX_MEMORY=268435456
ENV TT_MEMTX_MAX_TUPLE_SIZE=1048576
ENV TT_FORCE_RECOVERY='true'
ENV TT_LOG_LEVEL='info'
ENV TT_INSTANCE_NAME='instance-001'

# Открываем порт 3301 для внешних подключений
EXPOSE 3301
WORKDIR /etc/tarantool/
# Запускаем Tarantool при старте контейнера
CMD ["tarantool", "/etc/tarantool/init.lua"]


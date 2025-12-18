#!/bin/bash
# Скрипт для сборки и загрузки Docker образа в Yandex Container Registry

set -e

# Цвета для вывода
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Сборка и загрузка Docker образа в Yandex Cloud ===${NC}\n"

# Проверка наличия Docker
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker не установлен!${NC}"
    echo "Установите Docker Desktop: https://www.docker.com/products/docker-desktop/"
    exit 1
fi

# Проверка наличия yc CLI
if ! command -v yc &> /dev/null; then
    echo -e "${YELLOW}⚠️  Yandex Cloud CLI (yc) не установлен${NC}"
    echo "Установите: https://cloud.yandex.ru/docs/cli/quickstart"
    echo ""
    read -p "Продолжить без yc? (можно загрузить образ через веб-интерфейс) [y/N]: " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
    USE_WEB_UPLOAD=true
else
    USE_WEB_UPLOAD=false
fi

# Запрос registry ID
if [ -z "$REGISTRY_ID" ]; then
    echo -e "${YELLOW}Введите Registry ID из Yandex Cloud:${NC}"
    echo "Найти можно в: Container Registry → ваш registry → ID"
    read -p "Registry ID: " REGISTRY_ID
fi

# Имя образа
IMAGE_NAME="office-schedule-bot"
IMAGE_TAG="${IMAGE_TAG:-latest}"
FULL_IMAGE_NAME="cr.yandex/${REGISTRY_ID}/${IMAGE_NAME}:${IMAGE_TAG}"

echo ""
echo -e "${GREEN}📦 Сборка Docker образа...${NC}"
docker build -t ${IMAGE_NAME}:${IMAGE_TAG} .

if [ "$USE_WEB_UPLOAD" = true ]; then
    echo ""
    echo -e "${YELLOW}📤 Сохранение образа в файл для загрузки через веб-интерфейс...${NC}"
    docker save ${IMAGE_NAME}:${IMAGE_TAG} | gzip > ${IMAGE_NAME}-${IMAGE_TAG}.tar.gz
    echo -e "${GREEN}✅ Образ сохранен в: ${IMAGE_NAME}-${IMAGE_TAG}.tar.gz${NC}"
    echo ""
    echo "Следующие шаги:"
    echo "1. Откройте Yandex Cloud Console → Container Registry → ваш registry"
    echo "2. Перейдите в Images → Upload"
    echo "3. Загрузите файл: ${IMAGE_NAME}-${IMAGE_TAG}.tar.gz"
    echo "4. После загрузки используйте образ: ${FULL_IMAGE_NAME}"
else
    echo ""
    echo -e "${GREEN}🔐 Настройка авторизации в Container Registry...${NC}"
    yc container registry configure-docker
    
    echo ""
    echo -e "${GREEN}🏷️  Тегирование образа...${NC}"
    docker tag ${IMAGE_NAME}:${IMAGE_TAG} ${FULL_IMAGE_NAME}
    
    echo ""
    echo -e "${GREEN}📤 Загрузка образа в Yandex Container Registry...${NC}"
    docker push ${FULL_IMAGE_NAME}
    
    echo ""
    echo -e "${GREEN}✅ Образ успешно загружен!${NC}"
    echo ""
    echo "Используйте этот путь при создании Serverless Container:"
    echo -e "${GREEN}${FULL_IMAGE_NAME}${NC}"
fi

echo ""
echo -e "${GREEN}🎉 Готово!${NC}"


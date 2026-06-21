# План конфигурации и имплементации решения

## 1. Подготовка

- Установить Docker, настроить доступ к container registry
- Настроить доступ к Kubernetes-кластеру
- Получить доступ к PostgreSQL и S3-хранилищу

---

## 2. Разработка

- Создать Node.js проект, установить зависимости
- Реализовать подключение к PostgreSQL через переменные окружения
- Реализовать SQL-запрос с JOIN'ами для связывания таблиц
- Реализовать запись результата в CSV и загрузку в S3
- Добавить логирование и обработку ошибок

---

## 3. Контейнеризация

- Создать Dockerfile на основе Node.js образа
- Собрать Docker-образ и загрузить в container registry

---

## 4. Подготовка Kubernetes

- Создать CronJob с расписанием "0 6 * * *"
- Применить CronJob в кластер
- Проверить CronJob и расписание

## Пример конфигурации (манифест cronjob)

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: b2b-price-list-generator
  namespace: data-processing
spec:
  schedule: "0 6 * * *"
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 1
  jobTemplate:
    spec:
      backoffLimit: 3
      activeDeadlineSeconds: 600
      template:
        spec:
          restartPolicy: OnFailure
          containers:
            - name: price-generator
              image: node:18-alpine
              command: ["node", "/app/generator.js"]
              env:
                - name: DB_HOST
                  value: "postgres-service.data-processing.svc.cluster.local"
                - name: DB_PORT
                  value: "5432"
                - name: DB_NAME
                  value: "store_db"
                - name: DB_USER
                  value: "reader"
                - name: DB_PASSWORD
                  value: "password123"
                - name: S3_ENDPOINT
                  value: "https://s3.cloud.ru"
                - name: S3_ACCESS_KEY
                  value: "AKIAXXXXXX"
                - name: S3_SECRET_KEY
                  value: "XXXXXXXXXXXXXXX"
                - name: S3_BUCKET
                  value: "b2b-pricelists"
              resources:
                requests:
                  memory: "256Mi"
                  cpu: "100m"
                limits:
                  memory: "512Mi"
                  cpu: "500m"
              volumeMounts:
                - name: app-code
                  mountPath: /app
          volumes:
            - name: app-code
              configMap:
                name: price-generator-code
```

---

## 7. Настройка мониторинга

- Настроить Prometheus для сбора метрик
- Настроить алерты и уведомления при сбоях

---

## 8. Тестирование

- Выполнить ручной запуск Job
- Проверить логи, наличие CSV в S3 и корректность данных
- Проверить механизм retry при сбоях
# Базовый образ для сборки
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Копируем файл проекта и восстанавливаем зависимости
COPY *.csproj .
RUN dotnet restore

# Копируем остальные файлы и собираем приложение
COPY . .
RUN dotnet publish -c Release -o /app/publish

# Финальный образ
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app

# Копируем собранное приложение
COPY --from=build /app/publish .

# Указываем порт
ENV ASPNETCORE_URLS=http://+:5000
EXPOSE 5000

# Запускаем приложение
ENTRYPOINT ["dotnet", "Valuator.dll"]
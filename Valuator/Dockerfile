# Valuator/Dockerfile
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Копируем файлы проектов
COPY Shard/Shard.csproj Shard/
COPY Valuator/Valuator.csproj Valuator/

# Восстанавливаем зависимости
RUN dotnet restore Valuator/Valuator.csproj

# Копируем все исходники
COPY Shard/ Shard/
COPY Valuator/ Valuator/

# Собираем и публикуем Valuator
WORKDIR /src/Valuator
RUN dotnet publish -c Release -o /app/publish

# Финальный образ
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
EXPOSE 5000
ENV ASPNETCORE_URLS=http://+:5000
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "Valuator.dll"]
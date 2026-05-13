# RankCalculator/Dockerfile
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Копируем файлы проектов
COPY Shard/Shard.csproj Shard/
COPY RankCalculator/RankCalculator.csproj RankCalculator/

# Восстанавливаем зависимости
RUN dotnet restore RankCalculator/RankCalculator.csproj

# Копируем все исходники
COPY Shard/ Shard/
COPY RankCalculator/ RankCalculator/

# Собираем и публикуем RankCalculator
WORKDIR /src/RankCalculator
RUN dotnet publish -c Release -o /app/publish

# Финальный образ
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "RankCalculator.dll"]
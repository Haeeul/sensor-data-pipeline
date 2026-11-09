-- 여러 번 실행해도 안전하도록(idempotent) 존재 여부를 확인 후 생성
IF DB_ID('sensordb') IS NULL
    CREATE DATABASE sensordb;
GO

USE sensordb;
GO

IF OBJECT_ID('dbo.sensor_readings', 'U') IS NULL
CREATE TABLE dbo.sensor_readings (
    id           BIGINT IDENTITY(1,1) PRIMARY KEY,
    message_id   VARCHAR(64)  NOT NULL,
    sensor_id    VARCHAR(32)  NOT NULL,
    sensor_type  VARCHAR(16)  NOT NULL,
    value        FLOAT        NOT NULL,
    measured_at  DATETIME2(3) NOT NULL,                       -- 센서 측정 시각 (UTC)
    received_at  DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(), -- DB 저장 시각 (UTC)
    -- 중복 방지의 최종 방어선: QoS1 재전송/발행기 중복 모두 여기서 걸러짐
    CONSTRAINT UQ_sensor_readings_message_id UNIQUE (message_id)
);
GO

-- 다음 단계(조회 API)의 "센서별 기간 조회"용
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_sensor_readings_sensor_time')
    CREATE INDEX IX_sensor_readings_sensor_time
        ON dbo.sensor_readings (sensor_id, measured_at);
GO

IF OBJECT_ID('dbo.rejected_messages', 'U') IS NULL
CREATE TABLE dbo.rejected_messages (
    id           BIGINT IDENTITY(1,1) PRIMARY KEY,
    message_id   VARCHAR(64)   NULL,      -- 누락/파싱 실패 시 없을 수 있음
    reason       VARCHAR(32)   NOT NULL,
    detail       NVARCHAR(400) NULL,
    raw_payload  NVARCHAR(MAX) NOT NULL,  -- 원문 보존: 나중에 재처리/원인 분석용
    received_at  DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_rejected_messages_reason')
    CREATE INDEX IX_rejected_messages_reason
        ON dbo.rejected_messages (reason);
GO

CREATE TABLE abg.dbo.EventLog (
    "EventTimestamp" DATETIME2 NOT NULL,
    "Category" VARCHAR(50) NULL,
    "EventType" VARCHAR(50) NOT NULL,
    "Area" VARCHAR(50) NOT NULL,
    "Node" VARCHAR(50) NOT NULL,
    "Unit" VARCHAR(50) NULL,
    "Module" VARCHAR(50) NULL,
    "ModuleDescription" VARCHAR(255) NULL,
    "Parameter" VARCHAR(50) NOT NULL,
    "State" VARCHAR(50) NOT NULL,
    "Level" VARCHAR(50) NOT NULL,
    "Description1" VARCHAR(500) NULL,
    "Description2" VARCHAR(MAX) NULL
);

-- public."ApiIntegration" definition

-- Drop table

-- DROP TABLE public."ApiIntegration";

CREATE TABLE public."ApiIntegration" (
	id uuid NOT NULL,
	"name" varchar(255) NOT NULL,
	description varchar(255) NULL,
	manufacturer varchar(255) NULL,
	"serviceName" varchar(255) NULL,
	"createdAt" timestamptz DEFAULT now() NOT NULL,
	"updatedAt" timestamptz DEFAULT now() NOT NULL,
	"deletedAt" timestamptz NULL,
	CONSTRAINT "ApiIntegration_pkey" PRIMARY KEY (id)
);


-- public."Historic" definition

-- Drop table

-- DROP TABLE public."Historic";

CREATE TABLE public."Historic" (
	id uuid NOT NULL,
	"table" text NULL,
	"entityId" uuid NULL,
	"action" text NOT NULL,
	"userId" uuid NULL,
	"data" json NULL,
	"createdAt" timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT "Historic_pkey" PRIMARY KEY (id)
);
CREATE INDEX idx_historic_action ON public."Historic" USING btree (action);
CREATE INDEX idx_historic_action_created_at ON public."Historic" USING btree (action, "createdAt");
CREATE INDEX idx_historic_created_at ON public."Historic" USING btree ("createdAt");
CREATE INDEX idx_historic_data_gin ON public."Historic" USING gin (((data)::jsonb) jsonb_path_ops);
CREATE INDEX idx_historic_entity_table ON public."Historic" USING btree ("entityId", "table");
CREATE INDEX idx_historic_table ON public."Historic" USING btree ("table");
CREATE INDEX idx_historic_table_created_at ON public."Historic" USING btree ("table", "createdAt");
CREATE INDEX idx_historic_user_id ON public."Historic" USING btree ("userId");
CREATE INDEX idx_historic_user_id_created_at ON public."Historic" USING btree ("userId", "createdAt");


-- public."Organization" definition

-- Drop table

-- DROP TABLE public."Organization";

CREATE TABLE public."Organization" (
	id uuid NOT NULL,
	"name" text NOT NULL,
	cnpj text NOT NULL,
	address text NOT NULL,
	"admName" text NULL,
	"admRole" text NULL,
	"admPhone" text NULL,
	"admCellphone" text NULL,
	"admEmail" text NULL,
	"createdAt" timestamptz DEFAULT now() NOT NULL,
	"updatedAt" timestamptz DEFAULT now() NOT NULL,
	"deletedAt" timestamptz NULL,
	CONSTRAINT "Organization_pkey" PRIMARY KEY (id)
);


-- public."Role" definition

-- Drop table

-- DROP TABLE public."Role";

CREATE TABLE public."Role" (
	id uuid NOT NULL,
	"name" text NOT NULL,
	abbreviation text NOT NULL,
	"createdAt" timestamptz DEFAULT now() NOT NULL,
	"updatedAt" timestamptz NULL,
	"deletedAt" timestamptz NULL,
	CONSTRAINT "Role_pkey" PRIMARY KEY (id)
);


-- public."User" definition

-- Drop table

-- DROP TABLE public."User";

CREATE TABLE public."User" (
	id uuid NOT NULL,
	"name" text NOT NULL,
	username text NOT NULL,
	email text NOT NULL,
	"password" text NOT NULL,
	preferences json NULL,
	"createdAt" timestamptz DEFAULT now() NOT NULL,
	"updatedAt" timestamptz NULL,
	"deletedAt" timestamptz NULL,
	CONSTRAINT "User_email_key" UNIQUE (email),
	CONSTRAINT "User_pkey" PRIMARY KEY (id),
	CONSTRAINT "User_username_key" UNIQUE (username)
);


-- public."ApiAuthenticationMethod" definition

-- Drop table

-- DROP TABLE public."ApiAuthenticationMethod";

CREATE TABLE public."ApiAuthenticationMethod" (
	id uuid NOT NULL,
	"mode" public."enum_ApiAuthenticationMethod_mode" DEFAULT 'client'::"enum_ApiAuthenticationMethod_mode" NOT NULL,
	"type" public."enum_ApiAuthenticationMethod_type" DEFAULT 'apiKey'::"enum_ApiAuthenticationMethod_type" NOT NULL,
	"apiIntegrationId" uuid NOT NULL,
	"key" varchar(255) NULL,
	value varchar(255) NULL,
	url varchar(255) NULL,
	"token" varchar(255) NULL,
	"createdAt" timestamptz NOT NULL,
	"updatedAt" timestamptz NULL,
	"deletedAt" timestamptz NULL,
	CONSTRAINT "ApiAuthenticationMethod_key_key" UNIQUE (key),
	CONSTRAINT "ApiAuthenticationMethod_pkey" PRIMARY KEY (id),
	CONSTRAINT "ApiAuthenticationMethod_apiIntegrationId_fkey" FOREIGN KEY ("apiIntegrationId") REFERENCES public."ApiIntegration"(id) ON DELETE CASCADE ON UPDATE CASCADE
);


-- public."ApiIntegrationService" definition

-- Drop table

-- DROP TABLE public."ApiIntegrationService";

CREATE TABLE public."ApiIntegrationService" (
	id uuid NOT NULL,
	"name" varchar(255) NOT NULL,
	description varchar(255) NULL,
	"type" public."enum_ApiIntegrationService_type" NOT NULL,
	"requestPath" varchar(255) NULL,
	"requestMethod" public."enum_ApiIntegrationService_requestMethod" NOT NULL,
	"serviceName" varchar(255) NOT NULL,
	"serviceMethod" varchar(255) NOT NULL,
	"apiIntegrationId" uuid NOT NULL,
	"createdAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"updatedAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"deletedAt" timestamptz NULL,
	CONSTRAINT "ApiIntegrationService_pkey" PRIMARY KEY (id),
	CONSTRAINT "ApiIntegrationService_apiIntegrationId_fkey" FOREIGN KEY ("apiIntegrationId") REFERENCES public."ApiIntegration"(id) ON DELETE CASCADE ON UPDATE CASCADE
);


-- public."GatewayRestServer" definition

-- Drop table

-- DROP TABLE public."GatewayRestServer";

CREATE TABLE public."GatewayRestServer" (
	id uuid NOT NULL,
	url varchar(255) NOT NULL,
	"apiIntegrationId" uuid NULL,
	"createdAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"updatedAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"deletedAt" timestamptz NULL,
	CONSTRAINT "GatewayRestServer_pkey" PRIMARY KEY (id),
	CONSTRAINT "GatewayRestServer_apiIntegrationId_fkey" FOREIGN KEY ("apiIntegrationId") REFERENCES public."ApiIntegration"(id)
);


-- public."Module" definition

-- Drop table

-- DROP TABLE public."Module";

CREATE TABLE public."Module" (
	id uuid NOT NULL,
	"name" text NOT NULL,
	description text NULL,
	"type" public."enum_module_types" NOT NULL,
	"parentId" uuid NULL,
	"createdAt" timestamptz DEFAULT now() NOT NULL,
	"updatedAt" timestamptz NULL,
	"deletedAt" timestamptz NULL,
	icon text NULL,
	logo text NULL,
	CONSTRAINT "Module_pkey" PRIMARY KEY (id),
	CONSTRAINT "Module_parentId_fkey" FOREIGN KEY ("parentId") REFERENCES public."Module"(id)
);


-- public."Permission" definition

-- Drop table

-- DROP TABLE public."Permission";

CREATE TABLE public."Permission" (
	id uuid NOT NULL,
	entity text NOT NULL,
	"action" text NOT NULL,
	"roleId" uuid NOT NULL,
	"createdAt" timestamptz NOT NULL,
	"updatedAt" timestamptz NULL,
	"deletedAt" timestamptz NULL,
	CONSTRAINT "Permission_pkey" PRIMARY KEY (id),
	CONSTRAINT "Permission_roleId_fkey" FOREIGN KEY ("roleId") REFERENCES public."Role"(id) ON DELETE CASCADE ON UPDATE CASCADE
);


-- public."Status" definition

-- Drop table

-- DROP TABLE public."Status";

CREATE TABLE public."Status" (
	id uuid NOT NULL,
	"name" text NOT NULL,
	"isFinalStatus" bool DEFAULT false NOT NULL,
	description text NULL,
	"moduleId" uuid NULL,
	"createdAt" timestamptz DEFAULT now() NOT NULL,
	"updatedAt" timestamptz NULL,
	"deletedAt" timestamptz NULL,
	"isVisible" bool DEFAULT true NULL,
	CONSTRAINT "Status_pkey" PRIMARY KEY (id),
	CONSTRAINT "Status_moduleId_fkey" FOREIGN KEY ("moduleId") REFERENCES public."Module"(id) ON DELETE SET NULL ON UPDATE CASCADE
);


-- public."UserModuleStatusOrder" definition

-- Drop table

-- DROP TABLE public."UserModuleStatusOrder";

CREATE TABLE public."UserModuleStatusOrder" (
	id uuid NOT NULL,
	"userId" uuid NOT NULL,
	"moduleId" uuid NOT NULL,
	"statusOrder" text NULL,
	"createdAt" timestamptz NOT NULL,
	"updatedAt" timestamptz NOT NULL,
	CONSTRAINT "UserModuleStatusOrder_pkey" PRIMARY KEY (id),
	CONSTRAINT "UserModuleStatusOrder_moduleId_fkey" FOREIGN KEY ("moduleId") REFERENCES public."Module"(id) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT "UserModuleStatusOrder_userId_fkey" FOREIGN KEY ("userId") REFERENCES public."User"(id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE UNIQUE INDEX user_module_status_order_unique ON public."UserModuleStatusOrder" USING btree ("userId", "moduleId");


-- public."UserRole" definition

-- Drop table

-- DROP TABLE public."UserRole";

CREATE TABLE public."UserRole" (
	id uuid NOT NULL,
	"roleId" uuid NOT NULL,
	"userId" uuid NOT NULL,
	"createdAt" timestamptz DEFAULT now() NOT NULL,
	"updatedAt" timestamptz NULL,
	"deletedAt" timestamptz NULL,
	CONSTRAINT "UserRole_pkey" PRIMARY KEY (id),
	CONSTRAINT "UserRole_roleId_fkey" FOREIGN KEY ("roleId") REFERENCES public."Role"(id),
	CONSTRAINT "UserRole_userId_fkey" FOREIGN KEY ("userId") REFERENCES public."User"(id)
);


-- public."VisibilityStatusModule" definition

-- Drop table

-- DROP TABLE public."VisibilityStatusModule";

CREATE TABLE public."VisibilityStatusModule" (
	id uuid NOT NULL,
	"statusId" uuid NOT NULL,
	"moduleId" uuid NOT NULL,
	"isVisible" bool DEFAULT true NOT NULL,
	"createdAt" timestamptz DEFAULT now() NOT NULL,
	"updatedAt" timestamptz NULL,
	"deletedAt" timestamptz NULL,
	CONSTRAINT "VisibilityStatusModule_pkey" PRIMARY KEY (id),
	CONSTRAINT unique_status_module_constraint UNIQUE ("statusId", "moduleId"),
	CONSTRAINT "VisibilityStatusModule_moduleId_fkey" FOREIGN KEY ("moduleId") REFERENCES public."Module"(id) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT "VisibilityStatusModule_statusId_fkey" FOREIGN KEY ("statusId") REFERENCES public."Status"(id) ON DELETE CASCADE ON UPDATE CASCADE
);


-- public."Automation" definition

-- Drop table

-- DROP TABLE public."Automation";

CREATE TABLE public."Automation" (
	id uuid NOT NULL,
	"name" varchar(255) NOT NULL,
	description varchar(255) NULL,
	triggers json NOT NULL,
	actions json NOT NULL,
	active bool DEFAULT true NOT NULL,
	conditions json NULL,
	"moduleId" uuid NOT NULL,
	"createdAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"updatedAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"deletedAt" timestamptz NULL,
	CONSTRAINT "Automation_pkey" PRIMARY KEY (id),
	CONSTRAINT "Automation_moduleId_fkey" FOREIGN KEY ("moduleId") REFERENCES public."Module"(id)
);


-- public."Dashboard" definition

-- Drop table

-- DROP TABLE public."Dashboard";

CREATE TABLE public."Dashboard" (
	id uuid NOT NULL,
	"supersetDashboardId" uuid NOT NULL,
	title varchar(255) NULL,
	slug varchar(255) NULL,
	url varchar(255) NULL,
	thumbnail varchar(255) NULL,
	"moduleId" uuid NULL,
	"createdAt" timestamptz DEFAULT now() NOT NULL,
	"updatedAt" timestamptz NULL,
	"deletedAt" timestamptz NULL,
	CONSTRAINT "Dashboard_pkey" PRIMARY KEY (id),
	CONSTRAINT "Dashboard_moduleId_fkey" FOREIGN KEY ("moduleId") REFERENCES public."Module"(id)
);


-- public."DataSource" definition

-- Drop table

-- DROP TABLE public."DataSource";

CREATE TABLE public."DataSource" (
	id uuid NOT NULL,
	"name" text NOT NULL,
	description text NULL,
	"dataMap" json NULL,
	"entityName" text NOT NULL,
	"coverVisibleData" text NULL,
	"gatewayType" public."enum_DataSource_gatewayType" NOT NULL,
	"gatewayId" text NULL,
	"moduleId" uuid NOT NULL,
	"statusId" uuid NOT NULL,
	"createdAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"updatedAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"deletedAt" timestamptz NULL,
	"dailyLimit" int4 NULL,
	"wipEnabled" bool DEFAULT false NULL,
	"wipValue" int4 NULL,
	"voidStatusId" uuid NULL,
	CONSTRAINT "DataSource_pkey" PRIMARY KEY (id),
	CONSTRAINT "DataSource_moduleId_fkey" FOREIGN KEY ("moduleId") REFERENCES public."Module"(id),
	CONSTRAINT "DataSource_statusId_fkey" FOREIGN KEY ("statusId") REFERENCES public."Status"(id),
	CONSTRAINT "DataSource_voidStatusId_fkey" FOREIGN KEY ("voidStatusId") REFERENCES public."Status"(id)
);


-- public."Label" definition

-- Drop table

-- DROP TABLE public."Label";

CREATE TABLE public."Label" (
	id uuid NOT NULL,
	"name" text NOT NULL,
	description text NULL,
	"moduleId" uuid NULL,
	"createdAt" timestamptz DEFAULT now() NOT NULL,
	"updatedAt" timestamptz NULL,
	"deletedAt" timestamptz NULL,
	color text NULL,
	icon text NULL,
	"type" public."enum_Label_type" DEFAULT 'TEXT'::"enum_Label_type" NULL,
	"isVisible" bool DEFAULT true NOT NULL,
	CONSTRAINT "Label_pkey" PRIMARY KEY (id),
	CONSTRAINT "Label_moduleId_fkey" FOREIGN KEY ("moduleId") REFERENCES public."Module"(id) ON DELETE SET NULL ON UPDATE CASCADE
);


-- public."Membership" definition

-- Drop table

-- DROP TABLE public."Membership";

CREATE TABLE public."Membership" (
	id uuid NOT NULL,
	"userId" uuid NOT NULL,
	"moduleId" uuid NOT NULL,
	"createdAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"updatedAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"deletedAt" timestamptz NULL,
	CONSTRAINT "Membership_pkey" PRIMARY KEY (id),
	CONSTRAINT "Membership_moduleId_fkey" FOREIGN KEY ("moduleId") REFERENCES public."Module"(id) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT "Membership_userId_fkey" FOREIGN KEY ("userId") REFERENCES public."User"(id) ON DELETE CASCADE ON UPDATE CASCADE
);


-- public."Plugin" definition

-- Drop table

-- DROP TABLE public."Plugin";

CREATE TABLE public."Plugin" (
	id uuid NOT NULL,
	"dataSourceId" uuid NOT NULL,
	"moduleId" uuid NOT NULL,
	"apiIntegrationId" uuid NULL,
	"name" text NOT NULL,
	description text NULL,
	manufacturer text NULL,
	url text NOT NULL,
	"key" text NULL,
	value text NULL,
	"createdAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"updatedAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"deletedAt" timestamptz NULL,
	"serviceName" text NULL,
	"type" public."enum_Plugin_type" DEFAULT 'datasource'::"enum_Plugin_type" NULL,
	CONSTRAINT "Plugin_pkey" PRIMARY KEY (id),
	CONSTRAINT "Plugin_apiIntegrationId_fkey" FOREIGN KEY ("apiIntegrationId") REFERENCES public."ApiIntegration"(id) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT "Plugin_dataSourceId_fkey" FOREIGN KEY ("dataSourceId") REFERENCES public."DataSource"(id) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT "Plugin_moduleId_fkey" FOREIGN KEY ("moduleId") REFERENCES public."Module"(id) ON DELETE CASCADE ON UPDATE CASCADE
);


-- public."PluginDataSource" definition

-- Drop table

-- DROP TABLE public."PluginDataSource";

CREATE TABLE public."PluginDataSource" (
	id uuid NOT NULL,
	"pluginId" uuid NOT NULL,
	"dataSourceId" uuid NOT NULL,
	"createdAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"updatedAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"deletedAt" timestamptz NULL,
	CONSTRAINT "PluginDataSource_pkey" PRIMARY KEY (id),
	CONSTRAINT "PluginDataSource_dataSourceId_fkey" FOREIGN KEY ("dataSourceId") REFERENCES public."DataSource"(id) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT "PluginDataSource_pluginId_fkey" FOREIGN KEY ("pluginId") REFERENCES public."Plugin"(id) ON DELETE CASCADE ON UPDATE CASCADE
);


-- public."Ticket" definition

-- Drop table

-- DROP TABLE public."Ticket";

CREATE TABLE public."Ticket" (
	id uuid NOT NULL,
	"number" serial4 NOT NULL,
	"scheduleDate" timestamptz NULL,
	"data" json NULL,
	"parentId" uuid NULL,
	"dataSourceId" uuid NOT NULL,
	"moduleId" uuid NOT NULL,
	"userId" uuid NULL,
	"createdAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"updatedAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"deletedAt" timestamptz NULL,
	"scheduleDateEnd" timestamptz NULL,
	CONSTRAINT "Ticket_pkey" PRIMARY KEY (id),
	CONSTRAINT "Ticket_dataSourceId_fkey" FOREIGN KEY ("dataSourceId") REFERENCES public."DataSource"(id) ON DELETE RESTRICT ON UPDATE CASCADE,
	CONSTRAINT "Ticket_moduleId_fkey" FOREIGN KEY ("moduleId") REFERENCES public."Module"(id) ON DELETE RESTRICT ON UPDATE CASCADE,
	CONSTRAINT "Ticket_parentId_fkey" FOREIGN KEY ("parentId") REFERENCES public."Ticket"(id) ON DELETE RESTRICT ON UPDATE CASCADE,
	CONSTRAINT "Ticket_userId_fkey" FOREIGN KEY ("userId") REFERENCES public."User"(id) ON DELETE RESTRICT ON UPDATE CASCADE
);


-- public."TicketIntegration" definition

-- Drop table

-- DROP TABLE public."TicketIntegration";

CREATE TABLE public."TicketIntegration" (
	id uuid NOT NULL,
	"ticketId" uuid NOT NULL,
	"apiIntegrationId" uuid NOT NULL,
	"integrationData" json NULL,
	"createdAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"updatedAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"deletedAt" timestamptz NULL,
	CONSTRAINT "TicketIntegration_pkey" PRIMARY KEY (id),
	CONSTRAINT ticket_integration_unique UNIQUE ("ticketId", "apiIntegrationId"),
	CONSTRAINT "TicketIntegration_apiIntegrationId_fkey" FOREIGN KEY ("apiIntegrationId") REFERENCES public."ApiIntegration"(id) ON DELETE RESTRICT ON UPDATE CASCADE,
	CONSTRAINT "TicketIntegration_ticketId_fkey" FOREIGN KEY ("ticketId") REFERENCES public."Ticket"(id) ON DELETE RESTRICT ON UPDATE CASCADE
);


-- public."TicketLabel" definition

-- Drop table

-- DROP TABLE public."TicketLabel";

CREATE TABLE public."TicketLabel" (
	id uuid NOT NULL,
	"ticketId" uuid NOT NULL,
	"labelId" uuid NULL,
	"createdAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"updatedAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"deletedAt" timestamptz NULL,
	CONSTRAINT "TicketLabel_pkey" PRIMARY KEY (id),
	CONSTRAINT ticket_label_unique UNIQUE ("ticketId", "labelId"),
	CONSTRAINT "TicketLabel_labelId_fkey" FOREIGN KEY ("labelId") REFERENCES public."Label"(id) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT "TicketLabel_ticketId_fkey" FOREIGN KEY ("ticketId") REFERENCES public."Ticket"(id) ON DELETE CASCADE ON UPDATE CASCADE
);


-- public."TicketStatus" definition

-- Drop table

-- DROP TABLE public."TicketStatus";

CREATE TABLE public."TicketStatus" (
	id uuid NOT NULL,
	"ticketId" uuid NOT NULL,
	"statusId" uuid NOT NULL,
	"createdAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"updatedAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"deletedAt" timestamptz NULL,
	CONSTRAINT "TicketStatus_pkey" PRIMARY KEY (id),
	CONSTRAINT ticket_status_unique_constraint UNIQUE ("ticketId", "statusId"),
	CONSTRAINT "TicketStatus_statusId_fkey" FOREIGN KEY ("statusId") REFERENCES public."Status"(id) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT "TicketStatus_ticketId_fkey" FOREIGN KEY ("ticketId") REFERENCES public."Ticket"(id) ON DELETE CASCADE ON UPDATE CASCADE
);


-- public."Comment" definition

-- Drop table

-- DROP TABLE public."Comment";

CREATE TABLE public."Comment" (
	id uuid NOT NULL,
	"content" varchar(255) NOT NULL,
	"ticketId" uuid NOT NULL,
	"userId" uuid NOT NULL,
	"createdAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"updatedAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"deletedAt" timestamptz NULL,
	CONSTRAINT "Comment_pkey" PRIMARY KEY (id),
	CONSTRAINT "Comment_ticketId_fkey" FOREIGN KEY ("ticketId") REFERENCES public."Ticket"(id) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT "Comment_userId_fkey" FOREIGN KEY ("userId") REFERENCES public."User"(id) ON DELETE CASCADE ON UPDATE CASCADE
);


-- public."Occurrence" definition

-- Drop table

-- DROP TABLE public."Occurrence";

CREATE TABLE public."Occurrence" (
	id uuid NOT NULL,
	"content" varchar(255) NOT NULL,
	"ticketId" uuid NOT NULL,
	"userId" uuid NOT NULL,
	"createdAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"updatedAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"deletedAt" timestamptz NULL,
	CONSTRAINT "Occurrence_pkey" PRIMARY KEY (id),
	CONSTRAINT "Occurrence_ticketId_fkey" FOREIGN KEY ("ticketId") REFERENCES public."Ticket"(id) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT "Occurrence_userId_fkey" FOREIGN KEY ("userId") REFERENCES public."User"(id) ON DELETE CASCADE ON UPDATE CASCADE
);


-- public."PluginParameter" definition

-- Drop table

-- DROP TABLE public."PluginParameter";

CREATE TABLE public."PluginParameter" (
	id uuid NOT NULL,
	"ticketId" uuid NOT NULL,
	"pluginId" uuid NOT NULL,
	"data" json NULL,
	"createdAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"updatedAt" timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	"deletedAt" timestamptz NULL,
	CONSTRAINT "PluginParameter_pkey" PRIMARY KEY (id),
	CONSTRAINT "PluginParameter_pluginId_fkey" FOREIGN KEY ("pluginId") REFERENCES public."Plugin"(id) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT "PluginParameter_ticketId_fkey" FOREIGN KEY ("ticketId") REFERENCES public."Ticket"(id) ON DELETE CASCADE ON UPDATE CASCADE
);
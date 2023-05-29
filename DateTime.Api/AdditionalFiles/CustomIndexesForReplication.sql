USE [triovist_repl]
GO

SET ANSI_PADDING ON
GO

SET QUOTED_IDENTIFIER ON
GO

DROP INDEX IF EXISTS [_AccumRg21407_Custom1] ON [dbo].[_AccumRg21407]
GO
/****** Object:  Index [_AccumRg21407_Custom1]    Script Date: 26.05.2021 17:46:52 ******/
CREATE NONCLUSTERED INDEX [_AccumRg21407_Custom1] ON [dbo].[_AccumRg21407]
(
	[_Fld21408RRef] ASC,
	[_Period] ASC,
	[_RecorderTRef] ASC,
	[_RecorderRRef] ASC,
	[_LineNo] ASC,
	[_Fld21410_TYPE] ASC,
	[_Fld21410_RTRef] ASC,
	[_Fld21410_RRRef] ASC,
	[_Fld21411] ASC,
	[_Fld21442] ASC,
	[_Fld21443RRef] ASC,
	[_Fld21616] ASC,
	[_Fld21982] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

DROP INDEX IF EXISTS [_AccumRg21407_Custom2] ON [dbo].[_AccumRg21407]
GO
/****** Object:  Index [_AccumRg21407_Custom2]    Script Date: 21.09.2021 15:39:22 ******/
CREATE NONCLUSTERED INDEX [_AccumRg21407_Custom2] ON [dbo].[_AccumRg21407]
(
	[_Fld21408RRef] ASC,
	[_RecorderTRef] ASC,
	[_RecorderRRef] ASC
)
INCLUDE([_Fld21442],[_Fld21443RRef]) 
WHERE ([_Fld21410_TYPE]=0x08)
WITH (PAD_INDEX = ON, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 60, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

DROP INDEX IF EXISTS [CustomIndex_25104] ON [dbo].[_AccumRg25104]
GO
/****** Object:  Index [CustomIndex_25104]    Script Date: 26.05.2021 17:48:50 ******/
CREATE NONCLUSTERED INDEX [CustomIndex_25104] ON [dbo].[_AccumRg25104]
(
	[_Fld25105RRef] ASC,
	[_Period] ASC
)
INCLUDE([_RecordKind],[_Fld25107],[_Fld25108],[_Fld25201]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO


DROP INDEX IF EXISTS [CUSTOM_25110_1] ON [dbo].[_AccumRg25110]
GO
/****** Object:  Index [CUSTOM_25110_1]    Script Date: 26.05.2021 17:49:42 ******/
CREATE NONCLUSTERED INDEX [CUSTOM_25110_1] ON [dbo].[_AccumRg25110]
(
	[_Fld25111RRef] ASC,
	[_Period] ASC,
	[_Fld25202] ASC,
	[_Fld25112RRef] ASC
)
INCLUDE([_RecorderTRef],[_RecorderRRef],[_LineNo],[_Active],[_RecordKind],[_Fld25113],[_Fld25203]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

DROP INDEX IF EXISTS [CUSTOM_25110_2] ON [dbo].[_AccumRg25110]
GO

CREATE NONCLUSTERED INDEX [CUSTOM_25110_2] ON [dbo].[_AccumRg25110] ([_Period])
INCLUDE ([_RecordKind],[_Fld25111RRef],[_Fld25112RRef],[_Fld25202],[_Fld25203],[_Fld25113])
GO

-- 21век.Левковский 23.05.2023 Старт DEV1C-88090
DROP INDEX IF EXISTS [CUSTOM_25110_3] ON [dbo].[_AccumRg25110]
GO
CREATE NONCLUSTERED INDEX [CUSTOM_25110_3]
ON [dbo].[_AccumRg25110] ([_Fld25112RRef],[_Period])
INCLUDE ([_RecordKind],[_Fld25111RRef],[_Fld25202],[_Fld25203],[_Fld25113],[_Fld30385])
GO

DROP INDEX IF EXISTS [CUSTOM_25110_4] ON [dbo].[_AccumRg25110]
GO
CREATE NONCLUSTERED INDEX [CUSTOM_25110_4]
ON [dbo].[_AccumRg25110] ([_Fld25111RRef],[_Fld25112RRef],[_Period])
INCLUDE ([_RecordKind],[_Fld25202],[_Fld25203],[_Fld25113],[_Fld30385])
GO
-- 21век.Левковский 23.05.2023 Финиш DEV1C-88090

DROP INDEX IF EXISTS [_AccumRgT21444_1] ON [dbo].[_AccumRgT21444]
GO
/****** Object:  Index [_AccumRgT21444_1]    Script Date: 26.05.2021 17:55:23 ******/
CREATE CLUSTERED INDEX [_AccumRgT21444_1] ON [dbo].[_AccumRgT21444]
(
	[_Period] ASC,
	[_Fld21408RRef] ASC,
	[_Fld21410_TYPE] ASC,
	[_Fld21410_RTRef] ASC,
	[_Fld21410_RRRef] ASC,
	[_Fld23568RRef] ASC,
	[_Fld21424] ASC,
	[_Splitter] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

-- 21век.Левковский 03.05.2023 Старт DEV1C-88090
DROP INDEX IF EXISTS [_AccumRgT21444_2] ON [dbo].[_AccumRgT21444]
GO
CREATE NONCLUSTERED INDEX _AccumRgT21444_2
ON [dbo].[_AccumRgT21444] ([_Period],[_Fld21424])
INCLUDE ([_Fld21411],[_Fld21412])
GO
-- 21век.Левковский 03.05.2023 Финиш DEV1C-88090

DROP INDEX IF EXISTS [_InfoRg21711_Custom1] ON [dbo].[_InfoRg21711]
GO
/****** Object:  Index [_InfoRg21711_Custom1]    Script Date: 07.06.2021 0:40:22 ******/
CREATE NONCLUSTERED INDEX [_InfoRg21711_Custom1] ON [dbo].[_InfoRg21711]
(
	[_Fld25549] ASC,
	[_Fld26708RRef] ASC
)INCLUDE([_Period])WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

DROP INDEX IF EXISTS [_InfoRg22353_1] ON [dbo].[_InfoRg22353]
GO
/****** Object:  Index [_InfoRg22353_1]    Script Date: 26.05.2021 18:04:08 ******/
CREATE NONCLUSTERED INDEX [_InfoRg22353_1] ON [dbo].[_InfoRg22353]
(
	[_Fld22354] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

DROP INDEX IF EXISTS [_InfoRg23320_Custom1] ON [dbo].[_InfoRg23320]
GO
/****** Object:  Index [_InfoRg23320_Custom1]    Script Date: 07.06.2021 0:40:22 ******/
CREATE NONCLUSTERED INDEX [_InfoRg23320_Custom1] ON [dbo].[_InfoRg23320]
(
	[_Fld23322RRef] ASC,
	[_Fld23321] ASC,
	[_Fld23333] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

DROP INDEX IF EXISTS [_InfoRg23830_Custom1] ON [dbo].[_InfoRg23830]
GO
/****** Object:  Index [_InfoRg23830_Custom1]    Script Date: 01.06.2021 13:48:04 ******/
CREATE NONCLUSTERED INDEX [_InfoRg23830_Custom1] ON [dbo].[_InfoRg23830]
(
	[_Fld23833RRef] ASC,
	[_Fld23832] ASC,
	[_Fld23831RRef] ASC,
	[_Fld23834] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

-- 21век.Левковский 03.05.2023 Старт DEV1C-88090
DROP INDEX IF EXISTS [_InfoRg23830_Custom2] ON [dbo].[_InfoRg23830]
GO
/****** Object:  Index [_InfoRg23830_Custom2]    Script Date: 24.06.2021 16:53:45 ******/
CREATE NONCLUSTERED INDEX [_InfoRg23830_Custom2] ON [dbo].[_InfoRg23830]
(
	[_Fld23833RRef] ASC,
	[_Fld23832] DESC
)
INCLUDE([_Fld23831RRef],[_Fld23834]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
-- 21век.Левковский 03.05.2023 Финиш DEV1C-88090

DROP INDEX IF EXISTS [_Reference112_5] ON [dbo].[_Reference112]
GO
/****** Object:  Index [_Reference112_5]    Script Date: 26.05.2021 18:05:16 ******/
CREATE NONCLUSTERED INDEX [_Reference112_5] ON [dbo].[_Reference112]
(
	[_Fld25155] ASC,
	[_IDRRef] ASC,
	[_Fld2785RRef] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

DROP INDEX IF EXISTS [_Reference112_6] ON [dbo].[_Reference112]
GO
/****** Object:  Index [_Reference112_6]    Script Date: 26.05.2021 18:05:27 ******/
CREATE NONCLUSTERED INDEX [_Reference112_6] ON [dbo].[_Reference112]
(
	[_Fld25552] ASC,
	[_IDRRef] ASC,
	[_Fld2785RRef] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

DROP INDEX IF EXISTS [_Reference149_Custom1] ON [dbo].[_Reference149]
GO
CREATE NONCLUSTERED INDEX [_Reference149_Custom1] ON [dbo].[_Reference149]
(
	[_Code] ASC
)INCLUDE ([_IDRRef],[_Fld3480],[_Fld3489RRef],[_Fld3526RRef]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

DROP INDEX IF EXISTS [_Reference149_Custom2] ON [dbo].[_Reference149]
GO
CREATE NONCLUSTERED INDEX [_Reference149_Custom2] ON [dbo].[_Reference149]
(
	[_Fld3480] ASC
)INCLUDE ([_IDRRef],[_Code],[_Fld3489RRef],[_Fld3526RRef]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

DROP INDEX IF EXISTS [_Reference226_Custom1] ON [dbo].[_Reference226]
GO
/****** Object:  Index [_Reference226_6]    Script Date: 26.05.2021 18:06:37 ******/
CREATE NONCLUSTERED INDEX [_Reference226_Custom1] ON [dbo].[_Reference226]
(
	[_Fld19544] ASC
) INCLUDE([_IDRRef]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

DROP INDEX IF EXISTS [_Reference256_Custom2] ON [dbo].[_Reference256]
GO
/****** Object:  Index [_Reference256_2]    Script Date: 26.05.2021 18:07:01 ******/
CREATE NONCLUSTERED INDEX [_Reference256_Custom2] ON [dbo].[_Reference256]
(
	[_Marked] ASC,
	[_OwnerID_TYPE] ASC,
	[_OwnerID_RTRef] ASC,
	[_OwnerID_RRRef] ASC,
	[_Fld6003RRef] ASC
)
INCLUDE([_IDRRef],[_Fld6000],[_Fld6006],[_Fld6001],[_Fld6002],[_Fld6009]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
GO

DROP INDEX IF EXISTS [_Reference114_VT23370_SK] ON [dbo].[_Reference114_VT23370]
GO
/****** Object:  Index [_Reference114_VT23370_SK]    Script Date: 07.06.2021 0:40:21 ******/
CREATE CLUSTERED INDEX [_Reference114_VT23370_SK] ON [dbo].[_Reference114_VT23370]
(
	[_Reference114_IDRRef] ASC,
	[_KeyField] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

DROP INDEX IF EXISTS [_Reference114_VT25126_SK] ON [dbo].[_Reference114_VT25126]
GO
/****** Object:  Index [_Reference114_VT25126_SK]    Script Date: 07.06.2021 0:40:21 ******/
CREATE CLUSTERED INDEX [_Reference114_VT25126_SK] ON [dbo].[_Reference114_VT25126]
(
	[_Reference114_IDRRef] ASC,
	[_KeyField] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

-- 21век.Левковский 27.03.2023 Старт DEV1C-75871
DROP INDEX IF EXISTS [_Reference114_VT30388_SK] ON [dbo].[_Reference114_VT30388]
GO
/****** Object:  Index [_Reference114_VT30388_SK]    Script Date: 07.06.2021 0:40:21 ******/
CREATE CLUSTERED INDEX [_Reference114_VT30388_SK] ON [dbo].[_Reference114_VT30388]
(
	[_Reference114_IDRRef] ASC,
	[_KeyField] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
-- 21век.Левковский 27.03.2023 Финиш DEV1C-75871

DROP INDEX IF EXISTS [_Reference23294_VT23309_SK] ON [dbo].[_Reference23294_VT23309]
GO
/****** Object:  Index [_Reference23294_VT23309_SK]    Script Date: 07.06.2021 0:40:22 ******/
CREATE CLUSTERED INDEX [_Reference23294_VT23309_SK] ON [dbo].[_Reference23294_VT23309]
(
	[_Reference23294_IDRRef] ASC,
	[_KeyField] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

DROP INDEX IF EXISTS [_Reference23612_VT23613_SK] ON [dbo].[_Reference23612_VT23613]
GO
/****** Object:  Index [_Reference23612_VT23613_SK]    Script Date: 07.06.2021 0:40:22 ******/
CREATE CLUSTERED INDEX [_Reference23612_VT23613_SK] ON [dbo].[_Reference23612_VT23613]
(
	[_Reference23612_IDRRef] ASC,
	[_KeyField] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

DROP INDEX IF EXISTS [_Reference23612_VT23613_Custom1] ON [dbo].[_Reference23612_VT23613]
GO
/****** Object:  Index [_Reference23612_VT23613_Custom1]    Script Date: 08.06.2021 17:54:12 ******/
CREATE NONCLUSTERED INDEX [_Reference23612_VT23613_Custom1] ON [dbo].[_Reference23612_VT23613]
(
	[_Reference23612_IDRRef] ASC,
	[_Fld25265] ASC,
	[_Fld23615] ASC
)
INCLUDE([_Fld23617],[_Fld23618]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

DROP INDEX IF EXISTS [_Reference226_Custom2] ON [dbo].[_Reference226]
GO
/****** Object:  Index [_Reference226_Custom1]    Script Date: 08.06.2021 17:56:57 ******/
CREATE NONCLUSTERED INDEX [_Reference226_Custom2] ON [dbo].[_Reference226]
(
	[_Fld23620RRef] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

DROP INDEX IF EXISTS [_Document317_Custom1] ON [dbo].[_Document317]
GO
/****** Object:  Index [Custom_Document317]    Script Date: 22.07.2021 13:41:04 ******/
CREATE NONCLUSTERED INDEX [_Document317_Custom1] ON [dbo].[_Document317]
(
	[_Date_Time] ASC,
	[_Number] ASC
)
INCLUDE([_IDRRef],[_Fld8205RRef],[_Fld8241RRef],[_Fld8243RRef],[_Fld8244],[_Fld8245],[_Fld8260RRef],[_Fld21917RRef],[_Fld21650],[_Fld25158],[_Fld25159]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

USE [triovist_repl]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/* Создаем таблицы аггрегации */
IF  EXISTS (SELECT *
FROM sys.objects
WHERE object_id = OBJECT_ID(N'[dbo].[DeliveryPowerAggregate]') AND type in (N'U'))
DROP TABLE [dbo].[DeliveryPowerAggregate]
GO

CREATE TABLE [dbo].[DeliveryPowerAggregate]
(
	[Период] [datetime] NOT NULL,
	[ЗонаДоставки] [binary](16) NOT NULL,
	[МассаОборот] [numeric](10, 3) NOT NULL,
	[ОбъемОборот] [numeric](10, 3) NOT NULL,
	[ВремяНаОбслуживаниеОборот] [numeric](10, 3) NOT NULL,
	CONSTRAINT [PK_DeliveryPowerAggregate] PRIMARY KEY CLUSTERED 
(
	[Период] ASC,
	[ЗонаДоставки] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

IF  EXISTS (SELECT *
FROM sys.objects
WHERE object_id = OBJECT_ID(N'[dbo].[IntervalsAggregate]') AND type in (N'U'))
DROP TABLE [dbo].[IntervalsAggregate]
GO

CREATE TABLE [dbo].[IntervalsAggregate]
(
	[Период] [datetime] NOT NULL,
	[ГруппаПланирования] [binary](16) NOT NULL,
	[Геозона] [binary](16) NOT NULL,
	[ВремяНачала] [datetime] NOT NULL,
	[ВремяОкончания] [datetime] NOT NULL,
	[КоличествоЗаказовЗаИнтервалВремени] [numeric](10, 0) NOT NULL,
	CONSTRAINT [PK_IntervalsAggregate] PRIMARY KEY CLUSTERED 
(
	[Период] ASC,
	[ГруппаПланирования] ASC,
	[Геозона] ASC,
	[ВремяНачала] ASC,
	[ВремяОкончания] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

-- 21век.Левковский 16.05.2023 Старт DEV1C-88090
CREATE NONCLUSTERED INDEX [IntervalsAggregate_Custom1]
ON [dbo].[IntervalsAggregate] ([ГруппаПланирования],[Период],[КоличествоЗаказовЗаИнтервалВремени])
GO
-- 21век.Левковский 16.05.2023 Финиш DEV1C-88090

IF  EXISTS (SELECT *
FROM sys.objects
WHERE object_id = OBJECT_ID(N'[dbo].[WarehouseDatesAggregate]') AND type in (N'U'))
DROP TABLE [dbo].[WarehouseDatesAggregate]
GO

CREATE TABLE [dbo].[WarehouseDatesAggregate]
(
	[СкладИсточника] [binary](16) NOT NULL,
	[СкладНазначения] [binary](16) NOT NULL,
	[ДатаПрибытия] [datetime] NOT NULL,
	[ДатаСобытия] [datetime] NOT NULL
) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [WarehouseDatesAggregate_Custom1] ON [dbo].[WarehouseDatesAggregate]
(
	[СкладИсточника] ASC,
	[ДатаСобытия] ASC
)
INCLUDE([СкладНазначения],[ДатаПрибытия]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [WarehouseDatesAggregate_Custom2]
ON [dbo].[WarehouseDatesAggregate] ([СкладНазначения],[ДатаСобытия])
INCLUDE ([СкладИсточника],[ДатаПрибытия]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

/* Создаем буферные таблицы */
IF  EXISTS (SELECT *
FROM sys.objects
WHERE object_id = OBJECT_ID(N'[dbo].[buffering_table_deliverypower]') AND type in (N'U'))
DROP TABLE [dbo].[buffering_table_deliverypower]
GO

CREATE TABLE [dbo].[buffering_table_deliverypower]
(
	[id] [numeric](18, 0) IDENTITY(1,1) NOT NULL,
	[Период] [date] NOT NULL,
	[ЗонаДоставки] [binary](16) NOT NULL,
	[МассаОборот] [numeric](10, 3) NOT NULL,
	[ОбъемОборот] [numeric](10, 3) NOT NULL,
	[ВремяНаОбслуживаниеОборот] [numeric](10, 3) NOT NULL,
	PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

IF  EXISTS (SELECT *
FROM sys.objects
WHERE object_id = OBJECT_ID(N'[dbo].[buffering_table_intervals]') AND type in (N'U'))
DROP TABLE [dbo].[buffering_table_intervals]
GO

CREATE TABLE [dbo].[buffering_table_intervals]
(
	[id] [numeric](18, 0) IDENTITY(1,1) NOT NULL,
	[Период] [datetime] NOT NULL,
	[ГруппаПланирования] [binary](16) NOT NULL,
	[Геозона] [binary](16) NOT NULL,
	[ВремяНачала] [datetime] NOT NULL,
	[ВремяОкончания] [datetime] NOT NULL,
	[КоличествоЗаказовЗаИнтервалВремени] [numeric](10, 0) NOT NULL,
	PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO




/* Создаем хранимки обновления аггрегаций */
CREATE OR ALTER   procedure [dbo].[spUpdateAggregateDeliveryPower]
as

begin
	set nocount on;
	set xact_abort on;

	create table #t
	(
		[Период] [date] ,
		[ЗонаДоставки] [binary](16) ,
		[МассаОборот] [numeric](10, 3) ,
		[ОбъемОборот] [numeric](10, 3) ,
		[ВремяНаОбслуживаниеОборот] [numeric](10, 3)
	);

	if @@trancount > 0
  begin
		raiserror('Outer transaction detected', 16, 1);
		return;
	end;

	begin tran;

	declare @result int;
	DECLARE @exec_count int;
	set @exec_count = 5;
	WHILE @exec_count > 0 AND @result < 0
    BEGIN
		set @exec_count = @exec_count +1;
		exec @result = sys.sp_getapplock @Resource = N'[dbo].[buffering_table_deliverypower]', @LockMode = 'Exclusive', @LockOwner = 'Transaction', @LockTimeout = 1000;
	END;

	if @result < 0
	THROW 51000, 'Cant get lock for delete', 1;

	delete from [dbo].[buffering_table_deliverypower] 
	output deleted.[Период],deleted.[ЗонаДоставки],deleted.[МассаОборот],deleted.[ОбъемОборот],deleted.[ВремяНаОбслуживаниеОборот] into #t;

	with
		s ([Период], [ЗонаДоставки], [МассаОборот_delta], [ОбъемОборот_delta], [ВремяНаОбслуживаниеОборот_delta])
		as
		(
			select
				[Период], [ЗонаДоставки], sum([МассаОборот]), sum([ОбъемОборот]), sum([ВремяНаОбслуживаниеОборот])
			from
				#t
			Group by
	[Период],
	[ЗонаДоставки]
		)
 merge into [dbo].[DeliveryPowerAggregate] t
 using s on s.[Период] = t.[Период] and s.[ЗонаДоставки] = t.[ЗонаДоставки]
 when not matched then insert ([Период],[ЗонаДоставки],[МассаОборот],[ОбъемОборот],[ВремяНаОбслуживаниеОборот]) 
	values (s.[Период],s.[ЗонаДоставки], s.[МассаОборот_delta], s.[ОбъемОборот_delta],s.[ВремяНаОбслуживаниеОборот_delta])
 when matched then update set [МассаОборот] += s.[МассаОборот_delta], [ОбъемОборот] += s.[ОбъемОборот_delta], [ВремяНаОбслуживаниеОборот] += s.[ВремяНаОбслуживаниеОборот_delta];

	commit;
end;
GO

CREATE OR ALTER   procedure [dbo].[spUpdateAggregateIntervals]
as
begin
	set nocount on;
	set xact_abort on;

	create table #t
	(
		[Период] [datetime] NOT NULL,
		[ГруппаПланирования] [binary](16) NOT NULL,
		[Геозона] [binary](16) NOT NULL,
		[ВремяНачала] [datetime] NOT NULL,
		[ВремяОкончания] [datetime] NOT NULL,
		[КоличествоЗаказовЗаИнтервалВремени] [numeric](10, 0) NOT NULL
	);

	if @@trancount > 0
  begin
		raiserror('Outer transaction detected', 16, 1);
		return;
	end;

	begin tran;

	declare @result int;
	DECLARE @exec_count int;
	set @exec_count = 5;
	WHILE @exec_count > 0 AND @result < 0
    BEGIN
		set @exec_count = @exec_count +1;
		exec @result = sys.sp_getapplock @Resource = N'[dbo].[buffering_table_intervals]', @LockMode = 'Exclusive', @LockOwner = 'Transaction', @LockTimeout = 1000;
	END;

	if @result < 0
	THROW 51000, 'Cant get lock for delete', 1;

	delete from [dbo].[buffering_table_intervals] 
	output deleted.[Период],deleted.[ГруппаПланирования],deleted.[Геозона],deleted.[ВремяНачала],deleted.[ВремяОкончания], deleted.[КоличествоЗаказовЗаИнтервалВремени] into #t;

	with
		s ([Период], [ГруппаПланирования], [Геозона], [ВремяНачала], [ВремяОкончания], [КоличествоЗаказовЗаИнтервалВремени_delta] )
		as
		(
			select
				[Период], [ГруппаПланирования], [Геозона], [ВремяНачала], [ВремяОкончания], sum([КоличествоЗаказовЗаИнтервалВремени])
			from
				#t
			Group by
                [Период], [ГруппаПланирования], [Геозона], [ВремяНачала], [ВремяОкончания]
		)
 merge into [dbo].[IntervalsAggregate] t
 using s on s.[Период] = t.[Период]
		and s.[ГруппаПланирования] = t.[ГруппаПланирования]
		and s.[Геозона] = t.[Геозона]
		and s.[ВремяНачала] = t.[ВремяНачала]
		and s.[ВремяОкончания] = t.[ВремяОкончания]
 when not matched then insert ([Период], [ГруппаПланирования], [Геозона], [ВремяНачала], [ВремяОкончания], [КоличествоЗаказовЗаИнтервалВремени]) 
	values (s.[Период],s.[ГруппаПланирования], s.[Геозона], s.[ВремяНачала],s.[ВремяОкончания], s.[КоличествоЗаказовЗаИнтервалВремени_delta])
 when matched then update set [КоличествоЗаказовЗаИнтервалВремени] += s.[КоличествоЗаказовЗаИнтервалВремени_delta];

	commit;
end;
GO

CREATE OR ALTER    procedure [dbo].[spUpdateAggregateWarehouseDates]
as
begin
	set nocount on;
	set xact_abort on;


	if @@trancount > 0
  begin
		raiserror('Outer transaction detected', 16, 1);
		return;
	end;

	begin tran;

	with
		t ([СкладИсточника], [СкладНазначения], [ДатаПрибытия], [ДатаСобытия], RN)
		as
		(
			SELECT Distinct
				T1._Fld23831RRef AS [СкладИсточника],
				T1._Fld23833RRef AS [СкладНазначения],
				T1._Fld23834 AS [ДатаПрибытия],
				T1._Fld23832 AS [ДатаСобытия],
				ROW_NUMBER() OVER(Partition by _Fld23831RRef,_Fld23833RRef order by _Fld23834) as RN
			FROM
				dbo._InfoRg23830 T1

			Where T1._Fld23832 >= DateAdd(Minute, -5, DateAdd(YEAR,2000,GETDATE()))


			Group by 
		T1._Fld23831RRef,
		 T1._Fld23833RRef,
  T1._Fld23834,
  T1._Fld23832
		)
	select 
		IsNull(T1.[СкладИсточника], T2.[СкладИсточника]) AS [СкладИсточника],
		IsNull(T1.[СкладНазначения], T2.[СкладНазначения]) AS [СкладНазначения], 
		IsNull(T1.[ДатаПрибытия], T2.[ДатаПрибытия]) AS [ДатаПрибытия], 
		IsNull(T1.[ДатаСобытия], T2.[ДатаСобытия]) AS [ДатаСобытия], 
		case when T2.[ДатаПрибытия] is null then 1 else 0 end As newRecord, 
		case when T1.[ДатаПрибытия] is null then 1 else 0 end As deleteRecord
	Into #Temp_NewRecords
	from t T1
		Full join [dbo].[WarehouseDatesAggregate] T2
		ON T1.СкладИсточника = T2.СкладИсточника
			AND T1.СкладНазначения = T2.СкладНазначения
			AND T1.ДатаПрибытия = T2.ДатаПрибытия
			AND T1.ДатаСобытия = T2.ДатаСобытия
	where 
	Isnull(RN,1) <= 10 
	AND (T1.[ДатаПрибытия] is null OR T2.[ДатаПрибытия] is null);

	delete T1 from [dbo].[WarehouseDatesAggregate] T1 Inner join #Temp_NewRecords T2 ON T1.СкладИсточника = T2.СкладИсточника
			AND T1.СкладНазначения = T2.СкладНазначения
			AND T1.ДатаПрибытия = T2.ДатаПрибытия
			AND T1.ДатаСобытия = T2.ДатаСобытия 
			AND T2.deleteRecord = 1

	Insert Into [dbo].[WarehouseDatesAggregate]
	Select [СкладИсточника], [СкладНазначения], [ДатаПрибытия], [ДатаСобытия]
	From #Temp_NewRecords
	Where newRecord = 1;

	Drop Table #Temp_NewRecords;

	commit;
end;
GO

-- 21век.Левковский 23.05.2023 Старт DEV1C-88090
-- процедура для перестройки индексов
CREATE OR ALTER PROCEDURE [dbo].[spDefragmentTables]
    @FragmentationThreshold DECIMAL(5,2) = 30.0 -- Значение фрагментации таблицы, при котором будет выполняться дефрагментация.
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @SchemaName NVARCHAR(128);
    DECLARE @TableName NVARCHAR(128);
    DECLARE @IndexName NVARCHAR(128);
    DECLARE @FragmentationPercentage DECIMAL(5,2);
    DECLARE @SqlCommand NVARCHAR(MAX);
    
    -- Создаем курсор для перебора индексов с фрагментацией выше заданного порога
    DECLARE FragmentedIndexes CURSOR FOR
        SELECT OBJECT_SCHEMA_NAME(ips.object_id) AS SchemaName,
               OBJECT_NAME(ips.object_id) AS TableName,
               si.name AS IndexName,
               ips.avg_fragmentation_in_percent AS FragmentationPercentage
        FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
        INNER JOIN sys.indexes si ON ips.object_id = si.object_id AND ips.index_id = si.index_id
        WHERE ips.avg_fragmentation_in_percent > @FragmentationThreshold
          AND si.index_id > 0 -- Исключаем кучи (heap)
    
    OPEN FragmentedIndexes
    
    FETCH NEXT FROM FragmentedIndexes
    INTO @SchemaName, @TableName, @IndexName, @FragmentationPercentage
    
    WHILE @@FETCH_STATUS = 0
    BEGIN       
		-- Выполняем дефрагментацию индекса
        SET @SqlCommand = 'ALTER INDEX ' + QUOTENAME(@IndexName) + ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' REORGANIZE'
        EXEC sp_executesql @SqlCommand
        
        FETCH NEXT FROM FragmentedIndexes
        INTO @SchemaName, @TableName, @IndexName, @FragmentationPercentage
    END
    
    CLOSE FragmentedIndexes
    DEALLOCATE FragmentedIndexes
END
GO
-- 21век.Левковский 23.05.2023 Финиш DEV1C-88090

/*Создаем триггеры */
CREATE OR ALTER trigger [dbo].[_AccumRg25104_aggregate_trigger]
on [dbo].[_AccumRg25104]
after insert, update, delete
as
begin
	set nocount on;

	declare @result int;
	exec @result = sys.sp_getapplock @Resource = N'[dbo].[buffering_table_deliverypower]', @LockMode = 'Shared', @LockOwner = 'Transaction', @LockTimeout = -1;
	if @result < 0
	THROW 51000, 'Cant get lock for insert', 1;

	insert into [dbo].[buffering_table_deliverypower]
		([Период],[ЗонаДоставки],[МассаОборот],[ОбъемОборот],[ВремяНаОбслуживаниеОборот])
	select
		CAST(CAST([МощностиДоставки]._Period  AS DATE) AS DATETIME) AS [Период],
		[МощностиДоставки]._Fld25105RRef As [ЗонаДоставки],
		SUM(
            CASE
                WHEN ([МощностиДоставки]._RecordKind = 0.0) THEN [МощностиДоставки]._Fld25107
                ELSE -([МощностиДоставки]._Fld25107)
        END        
    ) AS [МассаОборот],
		SUM(
            CASE
                WHEN ([МощностиДоставки]._RecordKind = 0.0) THEN [МощностиДоставки]._Fld25108
                ELSE -([МощностиДоставки]._Fld25108)
        END        
    ) AS [ОбъемОборот],
		SUM(
            CASE
                WHEN ([МощностиДоставки]._RecordKind = 0.0) THEN [МощностиДоставки]._Fld25201
                ELSE -([МощностиДоставки]._Fld25201)
        END        
    ) AS [ВремяНаОбслуживаниеОборот]
	FROM
		inserted As [МощностиДоставки] With (READCOMMITTED)
	GROUP BY
    CAST(CAST([МощностиДоставки]._Period  AS DATE) AS DATETIME),
	[МощностиДоставки]._Fld25105RRef
	;

	insert into [dbo].[buffering_table_deliverypower]
		([Период],[ЗонаДоставки],[МассаОборот],[ОбъемОборот],[ВремяНаОбслуживаниеОборот])
	select
		CAST(CAST([МощностиДоставки]._Period  AS DATE) AS DATETIME) AS Период,
		[МощностиДоставки]._Fld25105RRef As [ЗонаДоставки],
		-1*SUM(
            CASE
                WHEN ([МощностиДоставки]._RecordKind = 0.0) THEN [МощностиДоставки]._Fld25107
                ELSE -([МощностиДоставки]._Fld25107)
        END        
    ) AS [МассаОборот],
		-1*SUM(
            CASE
                WHEN ([МощностиДоставки]._RecordKind = 0.0) THEN [МощностиДоставки]._Fld25108
                ELSE -([МощностиДоставки]._Fld25108)
        END        
    ) AS [ОбъемОборот],
		-1*SUM(
            CASE
                WHEN ([МощностиДоставки]._RecordKind = 0.0) THEN [МощностиДоставки]._Fld25201
                ELSE -([МощностиДоставки]._Fld25201)
        END        
    ) AS [ВремяНаОбслуживаниеОборот]
	FROM
		deleted As [МощностиДоставки] With (READCOMMITTED)
	GROUP BY
    CAST(CAST([МощностиДоставки]._Period  AS DATE) AS DATETIME),
	[МощностиДоставки]._Fld25105RRef
;

end;
GO


CREATE OR ALTER trigger [dbo].[_AccumRg25110_aggregate_trigger]
on [dbo].[_AccumRg25110]
after insert, update, delete
as
begin
	set nocount on;

	declare @result int;
	exec @result = sys.sp_getapplock @Resource = N'[dbo].[buffering_table_intervals]', @LockMode = 'Shared', @LockOwner = 'Transaction', @LockTimeout = -1;
	if @result < 0
	THROW 51000, 'Cant get lock for insert', 1;

	insert into [dbo].[buffering_table_intervals]
		([Период], [ГруппаПланирования], [Геозона], [ВремяНачала], [ВремяОкончания], [КоличествоЗаказовЗаИнтервалВремени])
	select
		T5._Period AS [Период],
		T5._Fld25112RRef As [ГруппаПланирования],
		T5._Fld25111RRef As [Геозона],
		T5._Fld25202 As [ВремяНачала],
		T5._Fld25203 As [ВремяОкончания],
		SUM(
                CASE
                    WHEN (T5._RecordKind = 0.0) THEN T5._Fld25113
                    ELSE -(T5._Fld25113)
                END
            ) AS [КоличествоЗаказовЗаИнтервалВремени]
	FROM
		inserted As T5 With (READCOMMITTED)
	GROUP BY
   T5._Period,
    T5._Fld25112RRef,
    T5._Fld25111RRef,
    T5._Fld25202,
	T5._Fld25203
	;

	insert into [dbo].[buffering_table_intervals]
		([Период], [ГруппаПланирования], [Геозона], [ВремяНачала], [ВремяОкончания], [КоличествоЗаказовЗаИнтервалВремени])
	select
		T5._Period AS [Период],
		T5._Fld25112RRef As [ГруппаПланирования],
		T5._Fld25111RRef As [Геозона],
		T5._Fld25202 As [ВремяНачала],
		T5._Fld25203 As [ВремяОкончания],
		-1*SUM(
                CASE
                    WHEN (T5._RecordKind = 0.0) THEN T5._Fld25113
                    ELSE -(T5._Fld25113)
                END
            ) AS [КоличествоЗаказовЗаИнтервалВремени]
	FROM
		deleted As T5 With (READCOMMITTED)
	GROUP BY
    T5._Period,
    T5._Fld25112RRef,
    T5._Fld25111RRef,
    T5._Fld25202,
	T5._Fld25203
;

end;
GO

/*Инициализируем таблицы аггрегации, очистив их и буферные таблицы перед этим*/
begin tran
alter table [dbo].[_AccumRg25104] disable trigger [_AccumRg25104_aggregate_trigger]
alter table [dbo].[_AccumRg25110] disable trigger [_AccumRg25110_aggregate_trigger]

delete  from [dbo].[buffering_table_deliverypower] with (tablock)
delete  from [dbo].[buffering_table_intervals] with (tablock)

delete  from [dbo].[IntervalsAggregate] with (tablock)

Insert into [dbo].[DeliveryPowerAggregate]
SELECT
	CAST(CAST([МощностиДоставки]._Period  AS DATE) AS DATETIME) AS [Период],
	[МощностиДоставки]._Fld25105RRef As [ЗонаДоставки],
	SUM(
            CASE
                WHEN ([МощностиДоставки]._RecordKind = 0.0) THEN [МощностиДоставки]._Fld25107
                ELSE -([МощностиДоставки]._Fld25107)
        END        
    ) AS [МассаОборот],
	SUM(
            CASE
                WHEN ([МощностиДоставки]._RecordKind = 0.0) THEN [МощностиДоставки]._Fld25108
                ELSE -([МощностиДоставки]._Fld25108)
        END        
    ) AS [ОбъемОборот],
	SUM(
            CASE
                WHEN ([МощностиДоставки]._RecordKind = 0.0) THEN [МощностиДоставки]._Fld25201
                ELSE -([МощностиДоставки]._Fld25201)
        END        
    ) AS [ВремяНаОбслуживаниеОборот]


FROM
	dbo._AccumRg25104 [МощностиДоставки] With (READCOMMITTED)
GROUP BY
    CAST(CAST([МощностиДоставки]._Period  AS DATE) AS DATETIME),
	[МощностиДоставки]._Fld25105RRef

Insert into [dbo].[IntervalsAggregate]
SELECT
	T5._Period AS [Период],
	T5._Fld25112RRef As [ГруппаПланирования],
	T5._Fld25111RRef As [Геозона],
	T5._Fld25202 As [ВремяНачала],
	T5._Fld25203 As [ВремяОкончания],
	SUM(
                CASE
                    WHEN (T5._RecordKind = 0.0) THEN T5._Fld25113
                    ELSE -(T5._Fld25113)
                END
            ) AS [КоличествоЗаказовЗаИнтервалВремени]

FROM
	dbo._AccumRg25110 T5 With (READCOMMITTED)
GROUP BY
    T5._Period,
    T5._Fld25112RRef,
    T5._Fld25111RRef,
    T5._Fld25202,
	T5._Fld25203

alter table [dbo].[_AccumRg25104] enable trigger [_AccumRg25104_aggregate_trigger]
alter table [dbo].[_AccumRg25110] enable trigger [_AccumRg25110_aggregate_trigger]
commit

/* Создадим джоб для обновления аггрегаций*/
USE [msdb]
GO

/****** Object:  Job [UpdateAggregates]    Script Date: 18.08.2021 16:31:51 ******/
DECLARE @jobId binary(16)

SELECT @jobId = job_id
FROM msdb.dbo.sysjobs
WHERE (name = N'UpdateAggregates')
IF (@jobId IS NOT NULL)
BEGIN
	EXEC msdb.dbo.sp_delete_job @jobId, @delete_unused_schedule=1
END
--EXEC msdb.dbo.sp_delete_job @job_id=N'7403ec1f-4359-46ea-81e2-e4a99fdf415c', @delete_unused_schedule=1
GO

/****** Object:  Job [UpdateAggregates]    Script Date: 18.08.2021 16:31:51 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Data Collector]    Script Date: 18.08.2021 16:31:52 ******/
IF NOT EXISTS (SELECT name
FROM msdb.dbo.syscategories
WHERE name=N'Data Collector' AND category_class=1)
BEGIN
	EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Data Collector'
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'UpdateAggregates', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Описание недоступно.', 
		@category_name=N'Data Collector', 
		@owner_login_name=N'netms', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [UpdateDeliveryPower]    Script Date: 18.08.2021 16:31:52 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'UpdateDeliveryPower', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'while 1 = 1
begin
exec spUpdateAggregateDeliveryPower
exec spUpdateAggregateIntervals
waitfor delay ''00:00:01''
end
', 
		@database_name=N'triovist_repl', 
		@flags=4
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'autostart schedule', 
		@enabled=1, 
		@freq_type=64, 
		@freq_interval=0, 
		@freq_subday_type=0, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20210818, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'4f5faf5d-a1cc-4bcc-b473-f513546274a0'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'recurring schedule', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=2, 
		@freq_subday_interval=10, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20210818, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'c2fa3a84-6176-4c19-88b8-febd8ef63a2a'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

USE [msdb]
GO

/****** Object:  Job [UpdateAggs1min]    Script Date: 23.08.2021 15:31:01 ******/
DECLARE @jobId binary(16)

SELECT @jobId = job_id
FROM msdb.dbo.sysjobs
WHERE (name = N'UpdateAggs1min')
IF (@jobId IS NOT NULL)
BEGIN
	EXEC msdb.dbo.sp_delete_job @jobId, @delete_unused_schedule=1
END
--EXEC msdb.dbo.sp_delete_job @job_id=N'3710e681-19c0-4db7-956d-52983ef83730', @delete_unused_schedule=1
GO

/****** Object:  Job [UpdateAggs1min]    Script Date: 23.08.2021 15:31:01 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Data Collector]    Script Date: 23.08.2021 15:31:01 ******/
IF NOT EXISTS (SELECT name
FROM msdb.dbo.syscategories
WHERE name=N'Data Collector' AND category_class=1)
BEGIN
	EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Data Collector'
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'UpdateAggs1min', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Описание недоступно.', 
		@category_name=N'Data Collector', 
		@owner_login_name=N'netms', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [cc]    Script Date: 23.08.2021 15:31:01 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'cc', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'exec [dbo].[spUpdateAggregateWarehouseDates]
', 
		@database_name=N'triovist_repl', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'autostart schedule', 
		@enabled=1, 
		@freq_type=64, 
		@freq_interval=0, 
		@freq_subday_type=0, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20210818, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'4f5faf5d-a1cc-4bcc-b473-f513546274a0'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'recurring schedule', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=4, 
		@freq_subday_interval=1, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20210818, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'c2fa3a84-6176-4c19-88b8-febd8ef63a2a'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO


USE [triovist_repl]
GO

CREATE OR ALTER     procedure [dbo].[spCheckAggregates]
as
begin
	set nocount on;
	set xact_abort on;

	SELECT
		T5._Period AS [Период],
		T5._Fld25112RRef As [ГруппаПланирования],
		T5._Fld25111RRef As [Геозона],
		T5._Fld25202 As [ВремяНачала],
		T5._Fld25203 As [ВремяОкончания],
		SUM(
					CASE
						WHEN (T5._RecordKind = 0.0) THEN T5._Fld25113
						ELSE -(T5._Fld25113)
					END
				) AS [КоличествоЗаказовЗаИнтервалВремени]
	into #Temp_IntervalsAll_old
	FROM
		dbo._AccumRg25110 T5 With (READCOMMITTED)
	Where T5._Period Between DateAdd(YEAR,2000,DateAdd(DAY,-1,GETDATE())) AND DateAdd(YEAR,2000,DateAdd(DAY,7,GETDATE()))
	GROUP BY
		T5._Period,
		T5._Fld25112RRef,
		T5._Fld25111RRef,
		T5._Fld25202,
		T5._Fld25203

	select
		Case when T1.[КоличествоЗаказовЗаИнтервалВремени] <> IsNull(T2.[КоличествоЗаказовЗаИнтервалВремени],9999999) then 1 else 0 End As CheckAgg,
		T1.[Период],
		T1.[ГруппаПланирования],
		T1.[Геозона],
		T1.[ВремяНачала],
		T1.[ВремяОкончания]
	Into #ErrorsIntervals
	From #Temp_IntervalsAll_old T1
		Left Join dbo.IntervalsAggregate T2 on
			T1.[Период] = T2.[Период]
			And T1.[ГруппаПланирования] = T2.[ГруппаПланирования]
			And T1.[Геозона] = T2.[Геозона]
			And T1.[ВремяНачала] = T2.[ВремяНачала]
			And T1.[ВремяОкончания] = T2.[ВремяОкончания]
	Where T1.[КоличествоЗаказовЗаИнтервалВремени] <> IsNull(T2.[КоличествоЗаказовЗаИнтервалВремени],9999999)


	SELECT
		CAST(CAST([МощностиДоставки]._Period  AS DATE) AS DATETIME) AS [Период],
		[МощностиДоставки]._Fld25105RRef As [ЗонаДоставки],
		SUM(
				CASE
					WHEN ([МощностиДоставки]._RecordKind = 0.0) THEN [МощностиДоставки]._Fld25107
					ELSE -([МощностиДоставки]._Fld25107)
			END        
		) AS [МассаОборот],
		SUM(
				CASE
					WHEN ([МощностиДоставки]._RecordKind = 0.0) THEN [МощностиДоставки]._Fld25108
					ELSE -([МощностиДоставки]._Fld25108)
			END        
		) AS [ОбъемОборот],
		SUM(
				CASE
					WHEN ([МощностиДоставки]._RecordKind = 0.0) THEN [МощностиДоставки]._Fld25201
					ELSE -([МощностиДоставки]._Fld25201)
			END        
		) AS [ВремяНаОбслуживаниеОборот]

	Into #DeliveryPowerOld
	FROM
		dbo._AccumRg25104 [МощностиДоставки] With (READCOMMITTED)
	GROUP BY
		CAST(CAST([МощностиДоставки]._Period  AS DATE) AS DATETIME),
		[МощностиДоставки]._Fld25105RRef


	select
		Case when T1.[ВремяНаОбслуживаниеОборот] <> IsNull(T2.[ВремяНаОбслуживаниеОборот],9999999) then 1 else 0 End As CheckAgg,
		Case when T1.[МассаОборот] <> IsNull(T2.[МассаОборот],9999999) then 1 else 0 End As CheckAgg1,
		Case when T1.[ОбъемОборот] <> IsNull(T2.[ОбъемОборот],9999999) then 1 else 0 End As CheckAgg2,
		T1.[Период],
		T1.[ЗонаДоставки]
	Into #ErrorsDeliveryPower
	From #DeliveryPowerOld T1
		Left Join dbo.DeliveryPowerAggregate T2 on
			T1.[Период] = T2.[Период]
			And T1.[ЗонаДоставки] = T2.[ЗонаДоставки]
	Where T1.[ВремяНаОбслуживаниеОборот] <> IsNull(T2.[ВремяНаОбслуживаниеОборот],9999999)
		Or T1.[МассаОборот] <> IsNull(T2.[МассаОборот],9999999)
		Or T1.[МассаОборот] <> IsNull(T2.[МассаОборот],9999999)

	SELECT
		T1._Fld23831RRef AS [СкладИсточника],
		T1._Fld23833RRef AS [СкладНазначения],
		MIN(T1._Fld23834) AS [ДатаПрибытия]
	Into #Temp_MinimumWarehouseDatesOld
	FROM
		dbo._InfoRg23830 T1 With (READCOMMITTED, INDEX([_InfoRg23830_Custom2]))
	WHERE
    
				T1._Fld23832 >= DateAdd(YEAR,2000,GETDATE())
	GROUP BY T1._Fld23831RRef,
	T1._Fld23833RRef

	SELECT
		T1.[СкладИсточника] AS [СкладИсточника],
		T1.[СкладНазначения] AS [СкладНазначения],
		MIN(T1.[ДатаПрибытия]) AS [ДатаПрибытия]
	Into #Temp_MinimumWarehouseDatesNew
	FROM
		[dbo].[WarehouseDatesAggregate] T1

	WHERE
	   T1.[ДатаСобытия] >= DateAdd(YEAR,2000,GETDATE())
	GROUP BY T1.[СкладИсточника],
	T1.[СкладНазначения]

	Select Top 1000
		#Temp_MinimumWarehouseDatesOld.[ДатаПрибытия] ,
		ISNULL(#Temp_MinimumWarehouseDatesNew.[ДатаПрибытия], GETDATE()) AS [ДатаПрибытияNew]
	Into #ErrorsWarehouseDates
	From
		#Temp_MinimumWarehouseDatesOld
		Left Join #Temp_MinimumWarehouseDatesNew On
			#Temp_MinimumWarehouseDatesOld.[СкладИсточника] = #Temp_MinimumWarehouseDatesNew.[СкладИсточника]
			And #Temp_MinimumWarehouseDatesOld.[СкладНазначения] = #Temp_MinimumWarehouseDatesNew.[СкладНазначения]
	Where #Temp_MinimumWarehouseDatesOld.[ДатаПрибытия] <> ISNULL(#Temp_MinimumWarehouseDatesNew.[ДатаПрибытия], GETDATE())


	Select Sum(t1.c1) as ErrorCount
	From (
							Select COUNT(*) AS c1
			from #ErrorsIntervals
		UNION All
			Select Count(*)
			From #ErrorsDeliveryPower
		UNION All
			Select Count(*)
			From #ErrorsWarehouseDates) As t1

end;
GO

-- 21век.Левковский 23.05.2023 Старт DEV1C-88090
USE [msdb]
GO

DECLARE @jobId binary(16)

SELECT @jobId = job_id
FROM msdb.dbo.sysjobs
WHERE (name = N'DefragmentTables')
IF (@jobId IS NOT NULL)
BEGIN
	EXEC msdb.dbo.sp_delete_job @jobId, @delete_unused_schedule=1
END
GO

/****** Object:  Job [DefragmentTables]    Script Date: 23.05.2023 17:54:45 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Database Maintenance]    Script Date: 23.05.2023 17:54:45 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Database Maintenance'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'DefragmentTables', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Дефрагментация индексов.', 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'netms', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [cc]    Script Date: 23.05.2023 17:54:45 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'cc', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'exec [dbo].[spDefragmentTables] @FragmentationThreshold = 30.0
', 
		@database_name=N'triovist_repl', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'every 5 minutes', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=4, 
		@freq_subday_interval=5, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20210818, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'c2fa3a84-6176-4c19-88b8-febd8ef63a2b'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO
-- 21век.Левковский 23.05.2023 Финиш DEV1C-88090
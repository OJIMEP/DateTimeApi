namespace DateTimeService.Application.Queries
{
    public static class AvailableDatePreliminaryCalculationQuery
    {
		public const string AvailableDatePickup =
@"
SELECT DISTINCT
	T1._IDRRef Номенклатура,
	T1._Code Код,
	T1._Fld3480 Артикул,
	T1._Fld3515RRef ТоварнаяКатегория
INTO #Temp_Goods
FROM dbo._Reference149 T1 WITH(NOLOCK) --Номенклатура
	INNER JOIN #Temp_GoodsRaw T2 WITH(NOLOCK)
	ON T1._Fld3480 = T2.Article
		AND T2.Code IS NULL

UNION ALL

SELECT DISTINCT
	T1._IDRRef,
	T1._Code,
	T1._Fld3480,
	T1._Fld3515RRef
FROM dbo._Reference149 T1 WITH(NOLOCK) --Номенклатура
	INNER JOIN #Temp_GoodsRaw T2 WITH(NOLOCK)
	ON T1._Code = T2.Code
--OPTION (KEEP PLAN, KEEPFIXED PLAN)
;
SELECT
	T1._Fld21408RRef Номенклатура,
	T1._Fld23568RRef Склад
INTO #Temp_Remains
FROM
	dbo._AccumRgT21444 T1 WITH(NOLOCK) --векРезервированиеТоваров.Остатки
	INNER JOIN #Temp_Goods T2 WITH(NOLOCK)
	ON T1._Fld21408RRef = T2.Номенклатура
		AND T1._Fld21410_TYPE = 0x08
		AND T1._Fld21410_RTRef = 0x000000E2
		AND T1._Period = '5999-11-01 00:00:00'
WHERE 
	T1._Fld21424 = @P_EmptyDate
GROUP BY 
	T1._Fld21408RRef,
	T1._Fld23568RRef
HAVING 
	SUM(T1._Fld21411 - T1._Fld21412) > 0
--OPTION (KEEP PLAN, KEEPFIXED PLAN, OPTIMIZE FOR UNKNOWN)
;

SELECT
	T1.Номенклатура,
	T1.Склад,
	@P_DateTimeNow ДатаДоступности
INTO #Temp_RemainsOnPickupPoints
FROM 
	#Temp_Remains T1 WITH(NOLOCK)
	INNER JOIN #Temp_PickupPoints T2 WITH(NOLOCK)
	ON T1.Склад = T2.ПунктВыдачи
--OPTION (KEEP PLAN, KEEPFIXED PLAN)
;

WITH PickupPointSourses AS (
	SELECT 
		T1._Fld33513RRef Склад
	FROM dbo._InfoRg33512 T1 WITH(NOLOCK) --СкладыДляРасчетаДатДоступностиТоваров
	WHERE 
		T1._Fld33514 = 0x01
		AND T1._Fld33515 = 0x01
)
SELECT
	T1._Fld33505RRef Номенклатура,
	T1._Fld33506RRef Склад,
	CASE 
		WHEN T3._Fld28349RRef IS NULL
		THEN T1._Fld33507 
	ELSE DATEADD(DAY, @P_DaysToShift, T3._Period) END ДатаДоступности
INTO #Temp_ClosestPickupDatesByGoods
FROM 
	dbo._InfoRg33504 T1 WITH(NOLOCK) --ДатыДоступностиТоваров
	INNER JOIN #Temp_Goods T2 WITH(NOLOCK)
	ON T1._Fld33505RRef = T2.Номенклатура
		AND T1._Fld33506RRef IN (SELECT T4.Склад FROM PickupPointSourses T4)
	LEFT JOIN dbo._InfoRg28348 AS T3 WITH(NOLOCK) --векПрослеживаемыеТоварныеКатегории
	ON @P_ApplyShifting = 1
		AND T2.ТоварнаяКатегория = T3._Fld28349RRef
		AND T1._Fld33507 BETWEEN T3._Period AND DateAdd(DAY, @P_DaysToShift, T3._Period)
	LEFT JOIN #Temp_RemainsOnPickupPoints T4 WITH(NOLOCK)
	ON T1._Fld33505RRef = T4.Номенклатура
WHERE 
	T1._Fld33507 >= @P_ActualDate
	AND T4.Номенклатура IS NULL

UNION ALL

SELECT 
	T1.Номенклатура,
	T1.Склад,
	CASE 
		WHEN T3._Fld28349RRef IS NULL
		THEN T1.ДатаДоступности 
	ELSE DATEADD(DAY, @P_DaysToShift, T3._Period) END
FROM #Temp_RemainsOnPickupPoints T1 WITH(NOLOCK)
	INNER JOIN #Temp_Goods T2 WITH(NOLOCK)
	ON T1.Номенклатура = T2.Номенклатура
	LEFT JOIN dbo._InfoRg28348 AS T3 WITH(NOLOCK) --векПрослеживаемыеТоварныеКатегории
	ON @P_ApplyShifting = 1
		AND T3._Fld28349RRef = T2.ТоварнаяКатегория 
		AND T1.ДатаДоступности BETWEEN T3._Period AND DateAdd(DAY, @P_DaysToShift, T3._Period)
--OPTION (KEEP PLAN, KEEPFIXED PLAN)
;

WITH AvailableDates AS (
    SELECT DISTINCT
        CAST(T1.ДатаДоступности AS DATE) ДатаНачала,
		CAST(DATEADD(DAY, @P_DaysToShow, T1.ДатаДоступности) AS DATE) ДатаОкончания
    FROM #Temp_ClosestPickupDatesByGoods T1 WITH(NOLOCK)
)
SELECT DISTINCT
    T1._Fld14262 Дата,
	DATEPART(WEEKDAY, T1._Fld14262) ДеньНедели
INTO #Temp_DateSeries
FROM dbo._InfoRg14260 T1 WITH(NOLOCK) --ДанныеПроизводственногоКалендаря
	INNER JOIN AvailableDates T2 WITH(NOLOCK)
    ON T1._Fld14261RRef = 0x8265002522BD9FAE11E4C0CE607941B6 -- календарь = Республика Беларусь
		AND T1._Fld14262 >= T2.ДатаНачала 
		AND T1._Fld14262 <= T2.ДатаОкончания
--OPTION (KEEP PLAN, KEEPFIXED PLAN)
;

WITH ScheduleChange AS (
	SELECT
		T1._Reference23612_IDRRef График,
		T1._Fld27056 Дата,
		T1._Fld27057 ВремяНачала,
		T1._Fld27058 ВремяОкончания,
		T1._Fld27059 Выходной
	FROM
		_Reference23612_VT27054 T1 WITH(NOLOCK) --векГрафикиРаботыСкладов.векТаблицаИзмененийРасписания
			INNER JOIN #Temp_DateSeries T2 WITH(NOLOCK)
			ON T1._Fld27056 = T2.Дата
)
SELECT 
    DATEADD(SECOND, DATEDIFF(SECOND, @P_EmptyDate, 
		ISNULL(T4.ВремяНачала, T1._Fld23617)), 
		T3.Дата) ВремяНачала,
    DATEADD(SECOND, DATEDIFF(SECOND, @P_EmptyDate, 
        ISNULL(T4.ВремяОкончания, T1._Fld23618)), 
        T3.Дата) ВремяОкончания,
    T2.ПунктВыдачи AS ПунктВыдачи
INTO #Temp_PickupWorkingHours
FROM dbo._Reference23612_VT23613 AS T1 WITH(NOLOCK) --векГрафикиРаботыСкладов.векТаблицаРасписание
	INNER JOIN #Temp_PickupPoints T2 WITH(NOLOCK)
	ON T1._Reference23612_IDRRef = T2.ГрафикРаботы
	INNER JOIN #Temp_DateSeries T3 WITH(NOLOCK)
	ON T3.ДеньНедели = T1._Fld23615
	LEFT JOIN ScheduleChange AS T4 WITH(NOLOCK)
	ON T1._Reference23612_IDRRef = T4.График
	AND T3.Дата = T4.Дата
WHERE ISNULL(T4.Выходной, T1._Fld25265) = 0x00
--OPTION (KEEP PLAN, KEEPFIXED PLAN)
;

WITH Sourses AS (
	SELECT DISTINCT 
		T1.Склад, 
		T1.ДатаДоступности 
	FROM #Temp_ClosestPickupDatesByGoods T1 WITH(NOLOCK)
)
SELECT
	T2.Склад СкладОтправления,
	T2.ДатаДоступности,
	T1.ПунктВыдачи СкладНазначения
INTO #Temp_PickupPointsPaths
FROM 
	#Temp_PickupPoints T1 WITH(NOLOCK)
	CROSS JOIN Sourses T2 WITH(NOLOCK)
--OPTION (KEEP PLAN, KEEPFIXED PLAN)
;

WITH PickupDateShift AS (
	SELECT
		T1._Fld25220RRef Склад,
		T1._Fld25221 РазмерСмещения
	FROM dbo._InfoRg25217 T1 WITH(NOLOCK) --векСмещениеДатДоставки
		INNER JOIN #Temp_PickupPoints PickupPoints WITH(NOLOCK)
		ON PickupPoints.ПунктВыдачи = T1._Fld25220RRef
			AND T1._Fld25218 <= @P_DateTimeNow
			AND T1._Fld25219 >= @P_DateTimeNow
)
SELECT
	T2.СкладОтправления,
	T2.СкладНазначения,
	T2.ДатаДоступности,
	MIN(DATEADD(HOUR, ISNULL(T3.РазмерСмещения, 0), T1._Fld33531)) ДатаПрибытия
INTO #Temp_PickupPointsDates
FROM 
	dbo._InfoRg33526 T1 WITH(NOLOCK) --векПрогнозныеДатыПоставокНаСклады
		INNER JOIN #Temp_PickupPointsPaths T2 WITH(NOLOCK)
		ON T1._Fld33527RRef = T2.СкладОтправления 
			AND T1._Fld33528RRef = T2.СкладНазначения
			AND T1._Fld33529 <= T2.ДатаДоступности 
			AND T1._Fld33530 > T2.ДатаДоступности 			
		LEFT JOIN PickupDateShift T3 WITH(NOLOCK)
		ON T2.СкладНазначения = T3.Склад
GROUP BY
	T2.СкладОтправления,
	T2.СкладНазначения,
	T2.ДатаДоступности

UNION ALL
	
SELECT DISTINCT
	T1.Склад,
	T1.Склад,
	T1.ДатаДоступности,
	T1.ДатаДоступности
FROM #Temp_RemainsOnPickupPoints T1
--OPTION (KEEP PLAN, KEEPFIXED PLAN)
;

SELECT 
	T1.Номенклатура,
	MIN(CASE 
			WHEN T3.ВремяНачала < T2.ДатаПрибытия 
			THEN T2.ДатаПрибытия
		ELSE
			T3.ВремяНачала
		END) ДатаПрибытия
INTO #Temp_AvailablePickup
FROM #Temp_ClosestPickupDatesByGoods T1 WITH(NOLOCK)
	INNER JOIN #Temp_PickupPointsDates T2 WITH(NOLOCK)
	ON T1.Склад = T2.СкладОтправления
		AND T1.ДатаДоступности = T2.ДатаДоступности
	INNER JOIN #Temp_PickupWorkingHours T3 WITH(NOLOCK)
	ON T2.СкладНазначения = T3.ПунктВыдачи
		AND T2.ДатаПрибытия < T3.ВремяОкончания
GROUP BY
	T1.Номенклатура
--OPTION (KEEP PLAN, KEEPFIXED PLAN)
;

SELECT 
	T1.Артикул Article,
	T1.Код Code,
	ISNULL(T2.ДатаПрибытия, @P_MaxDate) Self
FROM
	#Temp_Goods T1 WITH(NOLOCK)
	INNER JOIN #Temp_AvailablePickup T2 WITH(NOLOCK)
	ON T1.Номенклатура = T2.Номенклатура
--OPTION (KEEP PLAN, KEEPFIXED PLAN)
		";

		public const string AvailableDateDelivery =
@"
SELECT 
	T1._IDRRef Номенклатура,
	T1._Code Код,
	T1._Fld3480 Артикул,
	T1._Fld3515RRef ТоварнаяКатегория,
	T1._Fld3526RRef Габарит
INTO #Temp_Goods
FROM dbo._Reference149 T1 WITH(NOLOCK) --Номенклатура
	INNER JOIN #Temp_GoodsRaw T2 WITH(NOLOCK)
	ON T1._Fld3480 = T2.Article
		AND T2.Code IS NULL

UNION ALL

SELECT 
	T1._IDRRef,
	T1._Code,
	T1._Fld3480,
	T1._Fld3515RRef,
	T1._Fld3526RRef
FROM dbo._Reference149 T1 WITH(NOLOCK) --Номенклатура
	INNER JOIN #Temp_GoodsRaw T2 WITH(NOLOCK)
	ON T1._Code = T2.Code
OPTION (KEEP PLAN, KEEPFIXED PLAN)
;
WITH City AS (
	SELECT TOP 1
		T3._Fld26708RRef Геозона
	FROM (SELECT
			T1._Fld25549,
			MAX(T1._Period) Период 
		FROM dbo._InfoRg21711 T1 WITH(NOLOCK) --векРасстоянияАВ
		WHERE 
			T1._Fld26708RRef <> 0x00 
			AND T1._Fld25549 = @P_CityCode
		GROUP BY 
			T1._Fld25549) T2
		INNER JOIN dbo._InfoRg21711 T3 WITH(NOLOCK) --векРасстоянияАВ
		ON T2._Fld25549 = T3._Fld25549 
			AND T2.Период = T3._Period
),
Temp_Geodata AS (
	SELECT
		T3._ParentIDRRef ЗонаДоставки,
		T2.Геозона,
		T1._Description ГеозонаНаименование
	FROM dbo._Reference114 T1 WITH(NOLOCK) --инГеографическиеЗоны 
		INNER JOIN City T2 WITH(NOLOCK)
		ON T1._IDRRef = T2.Геозона
		INNER JOIN _Reference99 T3 WITH(NOLOCK) --ЗоныДоставки
		ON T1._Fld2847RRef = T3._IDRRef
)
SELECT 
	T3.Геозона,
	T3.ГеозонаНаименование,
	T3.ЗонаДоставки,
	T1._IDRRef ГруппаПланирования,
	T1._Description,
	DATEDIFF(SECOND, @P_EmptyDate, IsNull(T1._Fld25519, @P_EmptyDate)) ВремяНаПодготовкуПоставщик,
	DATEDIFF(SECOND, @P_EmptyDate, IsNull(T1._Fld25520, @P_EmptyDate)) ВремяНаПодготовкуПеремещение,
	T1._Fld23301RRef Габарит,
	T1._Fld23302RRef Склад,
	CASE WHEN @P_TimeNow >= T1._Fld25138 THEN 1 ELSE 0 END СтопНаСегодня,
	CASE WHEN @P_TimeNow >= T1._Fld25140 THEN 1 ELSE 0 END СтопНаЗавтра,
	T1._Fld25137 ВремяДоступностиНаСегодня,
	T1._Fld25139 ВремяДоступностиНаЗавтра
INTO #Temp_PlanningGroup
FROM dbo._Reference23294 T1 WITH(NOLOCK) --векГруппыПланирования
	INNER JOIN dbo._Reference23294_VT23309 T2 WITH(NOLOCK) -- векГруппыПланирования.ЗоныДоставки
	ON T1._IDRRef = T2._Reference23294_IDRRef
	INNER JOIN Temp_Geodata T3 WITH(NOLOCK)
	ON T2._Fld23311RRef = T3.ЗонаДоставки
WHERE 
	T1._Marked = 0x00
	AND T1._Fld25141 = 0x01 --участвует в расчете мощности
OPTION (KEEP PLAN, KEEPFIXED PLAN)
;

WITH DeliveryWarehouses AS (
	SELECT DISTINCT 
		T1.Склад
	FROM #Temp_PlanningGroup T1
)
SELECT
	T1._Fld33505RRef Номенклатура,
	T1._Fld33506RRef Склад,
	CASE 
		WHEN T3._Fld28349RRef IS NULL
		THEN T1._Fld33507 
		ELSE DATEADD(DAY, @P_DaysToShift, T3._Period) END ДатаДоступности,
	T1._Fld33510 ПеремещениеМеждуСкладами,
	T1._Fld33511 ПоступлениеОтПоставщика
INTO #Temp_ClosestDatesByGoods
FROM 
	dbo._InfoRg33504 T1 WITH(NOLOCK) --ДатыДоступностиТоваров
	INNER JOIN #Temp_Goods T2 WITH(NOLOCK)
	ON T1._Fld33505RRef = T2.Номенклатура
		AND T1._Fld33506RRef IN (SELECT T4.Склад FROM DeliveryWarehouses T4)
	LEFT JOIN dbo._InfoRg28348 AS T3 WITH(NOLOCK)
	ON @P_ApplyShifting = 1
		AND T3._Fld28349RRef = T2.ТоварнаяКатегория 
		AND T1._Fld33507 BETWEEN T3._Period AND DateAdd(DAY, @P_DaysToShift, T3._Period)
WHERE T1._Fld33507 >= @P_ActualDate
OPTION (KEEP PLAN, KEEPFIXED PLAN)
;

SELECT
    T1._Period Период,
    T1._Fld25112RRef ГруппаПланирования, 
	T1._Fld25111RRef Геозона,
	T1._Fld25202 ВремяНачала,
	T1._Fld25203 ВремяОкончания,
    DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, T1._Fld25202) AS NUMERIC(12)
        ),
        T1._Period
    ) ВремяНачалаСДатой
INTO #Temp_AllIntervals
FROM
    dbo._AccumRg25110 T1 WITH(NOLOCK) --векИнтервалыДоставки
    INNER JOIN #Temp_PlanningGroup T2 WITH(NOLOCK)
	ON T1._Fld25111RRef = T2.Геозона
		AND T1._Fld25112RRef = T2.ГруппаПланирования
WHERE
    T1._Period BETWEEN @P_DateTimePeriodBegin AND @P_DateTimePeriodEnd
GROUP BY
    T1._Period,
    T1._Fld25112RRef,
    T1._Fld25111RRef,
    T1._Fld25202,
	T1._Fld25203
HAVING
	CAST(
        SUM(
            CASE
                WHEN (T1._RecordKind = 0.0) THEN T1._Fld25113
                ELSE -(T1._Fld25113)
            END
        ) AS NUMERIC(16, 0)
    ) > 0.0
OPTION (HASH GROUP, 
OPTIMIZE FOR (@P_DateTimePeriodBegin='{0}', @P_DateTimePeriodEnd='{1}')
--, KEEP PLAN, KEEPFIXED PLAN
)
;

WITH Geozones AS (
	SELECT DISTINCT
		T1.Геозона
	FROM #Temp_AllIntervals T1 WITH(NOLOCK)
),
Intervals AS (
	SELECT DISTINCT
		T1.Геозона,
		T2._Fld25128 НачалоИнтервала,
		T2._Fld25129 ОкончаниеИнтервала
	FROM Geozones T1 WITH(NOLOCK)
	INNER JOIN _Reference114_VT25126 T2 WITH(NOLOCK) --инГеографическиеЗоны.ВременныеИнтервалы
	ON T1.Геозона = T2._Reference114_IDRRef
)
SELECT
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, T3.НачалоИнтервала) AS NUMERIC(12)
        ),
        T1.Период
    ) ВремяНачала,
    T1.Период,
    T1.ГруппаПланирования,
    T1.Геозона
INTO #Temp_Intervals
FROM #Temp_AllIntervals T1 WITH(NOLOCK)
	INNER JOIN Intervals T3 WITH(NOLOCK)
	ON T1.Геозона = T3.Геозона
		AND T1.ВремяНачала >= T3.НачалоИнтервала
		AND T1.ВремяНачала < T3.ОкончаниеИнтервала
	INNER JOIN #Temp_PlanningGroup T2 WITH(NOLOCK) 
	ON (T1.ГруппаПланирования = T2.ГруппаПланирования)
		AND (T3.НачалоИнтервала >= T2.ВремяДоступностиНаСегодня)
		AND (T2.СтопНаСегодня = 0)
WHERE
    T1.Период = @P_DateTimePeriodBegin
GROUP BY 
	T3.НачалоИнтервала,
	T1.Период,
	T1.ГруппаПланирования,
	T1.Геозона

UNION ALL 

SELECT
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, T3.НачалоИнтервала) AS NUMERIC(12)
        ),
        T1.Период
    ),
    T1.Период,
    T1.ГруппаПланирования,
    T1.Геозона
FROM #Temp_AllIntervals T1 WITH(NOLOCK)
	INNER JOIN Intervals T3 WITH(NOLOCK)
	ON T1.Геозона = T3.Геозона
		AND T1.ВремяНачала >= T3.НачалоИнтервала
		AND T1.ВремяНачала < T3.ОкончаниеИнтервала
	INNER JOIN #Temp_PlanningGroup T2 WITH(NOLOCK) 
	ON (T1.ГруппаПланирования = T2.ГруппаПланирования)
		AND (T3.НачалоИнтервала >= T2.ВремяДоступностиНаСегодня)
		AND (T2.СтопНаЗавтра = 0
			OR T3.НачалоИнтервала >= T2.ВремяДоступностиНаЗавтра)
WHERE
    T1.Период = DATEADD(DAY, 1, @P_DateTimePeriodBegin)
GROUP BY 
	T3.НачалоИнтервала,
	T1.Период,
	T1.ГруппаПланирования,
	T1.Геозона

UNION ALL 

SELECT
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, T3.НачалоИнтервала) AS NUMERIC(12)
        ),
        T1.Период
    ),
    T1.Период,
    T1.ГруппаПланирования,
    T1.Геозона
FROM #Temp_AllIntervals T1 WITH(NOLOCK)
	INNER JOIN Intervals T3 WITH(NOLOCK)
	ON T1.Геозона = T3.Геозона
		AND T1.ВремяНачала >= T3.НачалоИнтервала
		AND T1.ВремяНачала < T3.ОкончаниеИнтервала
WHERE
    T1.Период BETWEEN DATEADD(DAY, 2, @P_DateTimePeriodBegin) AND @P_DateTimePeriodEnd 
GROUP BY 
	T3.НачалоИнтервала,
	T1.Период,
	T1.ГруппаПланирования,
	T1.Геозона
OPTION (KEEP PLAN, KEEPFIXED PLAN)
;

WITH PrepareTime AS (
	SELECT 
		T1.Номенклатура,
		ISNULL(T2.ВремяНаПодготовкуПоставщик, 0) ВремяНаПодготовкуПоставщик,
		ISNULL(T2.ВремяНаПодготовкуПеремещение, 0) ВремяНаПодготовкуПеремещение
	FROM #Temp_Goods T1
		INNER JOIN #Temp_ClosestDatesByGoods T3
		ON T1.Номенклатура = T3.Номенклатура
		LEFT JOIN #Temp_PlanningGroup T2
		ON T3.Склад = T2.Склад
			AND (T1.Габарит = T2.Габарит
			OR (T1.Габарит = 0xAC2CBF86E693F63444670FFEB70264EE AND T2.Габарит = 0xAD3F7F5FC4F15DAD4F693CAF8365EC0D)) -- К и Х
)
SELECT
	T1.Номенклатура,
	T1.Склад,
	CASE
		WHEN T1.ПоступлениеОтПоставщика = 0x01
			THEN DATEADD(SECOND, T2.ВремяНаПодготовкуПоставщик, T1.ДатаДоступности)
		WHEN T1.ПеремещениеМеждуСкладами = 0x01
			THEN DATEADD(SECOND, T2.ВремяНаПодготовкуПеремещение, T1.ДатаДоступности)
        ELSE T1.ДатаДоступности
    END ДатаДоступности
INTO #Temp_ShipmentDates
FROM 
	#Temp_ClosestDatesByGoods T1 WITH(NOLOCK)
	INNER JOIN PrepareTime T2 WITH(NOLOCK)
	ON T1.Номенклатура = T2.Номенклатура
OPTION (KEEP PLAN, KEEPFIXED PLAN)
;

WITH Temp_DeliveryPower AS (
	SELECT
		SUM(
			CASE
				WHEN (T1._RecordKind = 0.0) THEN T1._Fld25107
				ELSE -(T1._Fld25107)
			END        
		) МассаОборот,    
		SUM(
			CASE
				WHEN (T1._RecordKind = 0.0) THEN T1._Fld25108
				ELSE -(T1._Fld25108)
			END        
		) ОбъемОборот,    
		SUM(
			CASE
				WHEN (T1._RecordKind = 0.0) THEN T1._Fld25201
				ELSE -(T1._Fld25201)
			END        
		) ВремяНаОбслуживаниеОборот,
		CAST(CAST(T1._Period  AS DATE) AS DATETIME) Дата
	FROM
		dbo._AccumRg25104 T1 WITH(NOLOCK) --векМощностиДоставки
		INNER JOIN #Temp_PlanningGroup T2 WITH(NOLOCK)
		ON T1._Fld25105RRef = T2.ЗонаДоставки
	WHERE
		T1._Period BETWEEN @P_DateTimePeriodBegin AND @P_DateTimePeriodEnd
	GROUP BY
		CAST(CAST(T1._Period  AS DATE) AS DATETIME)
)
SELECT
    T1.Номенклатура,
    MIN(
        ISNULL(
            T3.ВремяНачала,
			CASE
                WHEN (T1.ДатаДоступности > DATEADD(SECOND, -1, @P_DateTimePeriodEnd)) 
				THEN DATEADD(DAY, 1, CAST(CAST(T1.ДатаДоступности AS DATE) AS DATETIME))
                ELSE DATEADD(DAY, 1, @P_DateTimePeriodEnd)
            END
        )
    ) ДатаКурьерскойДоставки
INTO #Temp_AvailableCourier
FROM
    #Temp_ShipmentDates T1 WITH(NOLOCK)
    LEFT JOIN Temp_DeliveryPower T2 WITH(NOLOCK)
        INNER JOIN #Temp_Intervals T3 WITH(NOLOCK)
		ON T3.Период = T2.Дата
	ON T2.МассаОборот >= 1
		AND T2.ОбъемОборот >= 1
		AND T2.ВремяНаОбслуживаниеОборот >= 10
		AND T2.Дата >= CAST(CAST(T1.ДатаДоступности AS DATE) AS DATETIME)    
		AND T3.ВремяНачала >= T1.ДатаДоступности
GROUP BY
	T1.Номенклатура
OPTION (HASH GROUP, 
OPTIMIZE FOR (@P_DateTimePeriodBegin='{0}', @P_DateTimePeriodEnd='{1}')
--,KEEP PLAN, KEEPFIXED PLAN
)
;

WITH YourTimeInterval AS (
	SELECT TOP 1 
		DATEDIFF(MINUTE, T2._Fld30390, T2._Fld30391) ИнтервалВВашеВремя
	FROM 
		#Temp_PlanningGroup T1 WITH(NOLOCK)
		INNER JOIN dbo._Reference114_VT30388 T2 WITH(NOLOCK)
		ON T2._Reference114_IDRRef = T1.Геозона 
)
SELECT 
	T1.Артикул Article,
	T1.Код Code,
	T2.ДатаКурьерскойДоставки Courier,
	ISNULL(T3.ИнтервалВВашеВремя, 0) YourTimeInterval
From
	#Temp_Goods T1 WITH(NOLOCK)
	INNER JOIN #Temp_AvailableCourier T2 WITH(NOLOCK)
	ON T1.Номенклатура = T2.Номенклатура
	LEFT OUTER JOIN YourTimeInterval T3
	ON 1 = 1
OPTION (KEEP PLAN, KEEPFIXED PLAN)
";

        public const string AvailableDateDelivery1 =
@"
SELECT DISTINCT
	T1._IDRRef Номенклатура,
	T1._Code Код,
	T1._Fld3480 Артикул,
	T1._Fld3515RRef ТоварнаяКатегория,
	T1._Fld3526RRef Габарит
INTO #Temp_Goods
FROM dbo._Reference149 T1 WITH(NOLOCK) --Номенклатура
	INNER JOIN #Temp_GoodsRaw T2 WITH(NOLOCK)
	ON T1._Fld3480 = T2.Article
		AND T2.Code IS NULL

UNION ALL

SELECT DISTINCT
	T1._IDRRef,
	T1._Code,
	T1._Fld3480,
	T1._Fld3515RRef,
	T1._Fld3526RRef
FROM dbo._Reference149 T1 WITH(NOLOCK) --Номенклатура
	INNER JOIN #Temp_GoodsRaw T2 WITH(NOLOCK)
	ON T1._Code = T2.Code
OPTION (KEEP PLAN, KEEPFIXED PLAN)
;
WITH City AS (
	SELECT TOP 1
		T3._Fld26708RRef Геозона
	FROM (SELECT
			T1._Fld25549,
			MAX(T1._Period) Период 
		FROM dbo._InfoRg21711 T1 WITH(NOLOCK) --векРасстоянияАВ
		WHERE 
			T1._Fld26708RRef <> 0x00 
			AND T1._Fld25549 = @P_CityCode
		GROUP BY 
			T1._Fld25549) T2
		INNER JOIN dbo._InfoRg21711 T3 WITH(NOLOCK) --векРасстоянияАВ
		ON T2._Fld25549 = T3._Fld25549 
			AND T2.Период = T3._Period
),
Temp_Geodata AS (
	SELECT
		T3._ParentIDRRef ЗонаДоставки,
		T2.Геозона,
		T1._Description ГеозонаНаименование
	FROM dbo._Reference114 T1 WITH(NOLOCK) --инГеографическиеЗоны 
		INNER JOIN City T2 WITH(NOLOCK)
		ON T1._IDRRef = T2.Геозона
		INNER JOIN _Reference99 T3 WITH(NOLOCK) --ЗоныДоставки
		ON T1._Fld2847RRef = T3._IDRRef
)
SELECT 
	T3.Геозона,
	T3.ГеозонаНаименование,
	T3.ЗонаДоставки,
	T1._IDRRef ГруппаПланирования,
	T1._Description,
	DATEDIFF(SECOND, @P_EmptyDate, IsNull(T1._Fld25519, @P_EmptyDate)) ВремяНаПодготовкуПоставщик,
	DATEDIFF(SECOND, @P_EmptyDate, IsNull(T1._Fld25520, @P_EmptyDate)) ВремяНаПодготовкуПеремещение,
	T1._Fld23301RRef Габарит,
	T1._Fld23302RRef Склад,
	CASE WHEN @P_TimeNow >= T1._Fld25138 THEN 1 ELSE 0 END СтопНаСегодня,
	CASE WHEN @P_TimeNow >= T1._Fld25140 THEN 1 ELSE 0 END СтопНаЗавтра,
	T1._Fld25137 ВремяДоступностиНаСегодня,
	T1._Fld25139 ВремяДоступностиНаЗавтра
INTO #Temp_PlanningGroup
FROM dbo._Reference23294 T1 WITH(NOLOCK) --векГруппыПланирования
	INNER JOIN dbo._Reference23294_VT23309 T2 WITH(NOLOCK) -- векГруппыПланирования.ЗоныДоставки
	ON T1._IDRRef = T2._Reference23294_IDRRef
	INNER JOIN Temp_Geodata T3 WITH(NOLOCK)
	ON T2._Fld23311RRef = T3.ЗонаДоставки
WHERE 
	T1._Marked = 0x00
	AND T1._Fld25141 = 0x01 --участвует в расчете мощности
OPTION (KEEP PLAN, KEEPFIXED PLAN)
;

WITH DeliveryWarehouses AS (
	SELECT DISTINCT 
		T1.Склад
	FROM #Temp_PlanningGroup T1
)
SELECT
	T1._Fld33505RRef Номенклатура,
	T1._Fld33506RRef Склад,
	CASE 
		WHEN T3._Fld28349RRef IS NULL
		THEN T1._Fld33507 
		ELSE DATEADD(DAY, @P_DaysToShift, T3._Period) END ДатаДоступности,
	T1._Fld33510 ПеремещениеМеждуСкладами,
	T1._Fld33511 ПоступлениеОтПоставщика
INTO #Temp_ClosestDatesByGoods
FROM 
	dbo._InfoRg33504 T1 WITH(NOLOCK) --ДатыДоступностиТоваров
	INNER JOIN #Temp_Goods T2 WITH(NOLOCK)
	ON T1._Fld33505RRef = T2.Номенклатура
		AND T1._Fld33506RRef IN (SELECT T4.Склад FROM DeliveryWarehouses T4)
	LEFT JOIN dbo._InfoRg28348 AS T3 WITH(NOLOCK)
	ON @P_ApplyShifting = 1
		AND T3._Fld28349RRef = T2.ТоварнаяКатегория 
		AND T1._Fld33507 BETWEEN T3._Period AND DateAdd(DAY, @P_DaysToShift, T3._Period)
WHERE T1._Fld33507 >= @P_ActualDate
OPTION (KEEP PLAN, KEEPFIXED PLAN)
;
";

		public const string TempAllIntervals = @"
SELECT
	CAST(CAST(T1._Period  AS DATE) AS DATETIME) Дата
INTO #TempDeliveryPowersAvailable
FROM
	dbo._AccumRg25104 T1 WITH(NOLOCK) --векМощностиДоставки
	INNER JOIN #Temp_PlanningGroup T2 WITH(NOLOCK)
	ON T1._Fld25105RRef = T2.ЗонаДоставки
WHERE
	T1._Period BETWEEN @P_DateTimePeriodBegin AND @P_DateTimePeriodEnd
GROUP BY
	CAST(CAST(T1._Period  AS DATE) AS DATETIME)
HAVING
	SUM(
		CASE
			WHEN (T1._RecordKind = 0.0) THEN T1._Fld25107
			ELSE -(T1._Fld25107)
		END        
	) >= 100
	AND 
	SUM(
		CASE
			WHEN (T1._RecordKind = 0.0) THEN T1._Fld25108
			ELSE -(T1._Fld25108)
		END        
	) >= 10
	AND 
	SUM(
		CASE
			WHEN (T1._RecordKind = 0.0) THEN T1._Fld25201
			ELSE -(T1._Fld25201)
		END        
	) >= 10
OPTION (KEEP PLAN, KEEPFIXED PLAN)
;

SELECT
    T1._Period Период,
    T1._Fld25112RRef ГруппаПланирования, 
	T1._Fld25111RRef Геозона,
	T1._Fld25202 ВремяНачала,
	T1._Fld25203 ВремяОкончания,
    DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, T1._Fld25202) AS NUMERIC(12)
        ),
        T1._Period
    ) ВремяНачалаСДатой
INTO #Temp_AllIntervals
FROM
    dbo._AccumRg25110 T1 WITH(NOLOCK) --векИнтервалыДоставки
    INNER JOIN #Temp_PlanningGroup T2 WITH(NOLOCK)
	ON T1._Fld25111RRef = T2.Геозона
		AND T1._Fld25112RRef = T2.ГруппаПланирования
	INNER JOIN #TempDeliveryPowersAvailable T3
	ON T1._Period = T3.Дата
WHERE
    T1._Period BETWEEN @P_DateTimePeriodBegin AND @P_DateTimePeriodEnd
GROUP BY
    T1._Period,
    T1._Fld25112RRef,
    T1._Fld25111RRef,
    T1._Fld25202,
	T1._Fld25203
HAVING
	CAST(
        SUM(
            CASE
                WHEN (T1._RecordKind = 0.0) THEN T1._Fld25113
                ELSE -(T1._Fld25113)
            END
        ) AS NUMERIC(16, 0)
    ) > 0.0
OPTION (HASH GROUP, 
OPTIMIZE FOR (@P_DateTimePeriodBegin='{0}', @P_DateTimePeriodEnd='{1}')
--, KEEP PLAN, KEEPFIXED PLAN
)
;	
";

        public const string TempAllIntervalsAggregate =
@"
SELECT   
    T1.Период AS Дата
INTO #TempDeliveryPowersAvailable
FROM dbo.DeliveryPowerAggregate T1 WITH(NOLOCK) 
WHERE T1.Период BETWEEN @P_DateTimePeriodBegin AND @P_DateTimePeriodEnd
	AND EXISTS (
        SELECT 1 
        FROM #Temp_PlanningGroup T2 
        WHERE T1.ЗонаДоставки = T2.ЗонаДоставки
    )
	AND T1.МассаОборот >= 100
	AND T1.ОбъемОборот >= 10
	AND T1.ВремяНаОбслуживаниеОборот >= 10
OPTION (KEEP PLAN, KEEPFIXED PLAN)
;

SELECT
	T1.Период AS Период,
	T1.ГруппаПланирования As ГруппаПланирования, 
	T1.Геозона As Геозона,
	T1.ВремяНачала As ВремяНачала,
	T1.ВремяОкончания As ВремяОкончания,
	DATEADD(
		SECOND,
		CAST(
			DATEDIFF(SECOND, @P_EmptyDate, T1.ВремяНачала) AS NUMERIC(12)
		),
		T1.Период
	) As ВремяНачалаСДатой
INTO #Temp_AllIntervals
FROM
	dbo.IntervalsAggregate T1 With (NOLOCK)
	INNER JOIN #Temp_PlanningGroup T2  
	ON T1.Геозона = T2.Геозона
		AND T1.ГруппаПланирования = T2.ГруппаПланирования
	INNER JOIN #TempDeliveryPowersAvailable T3
	ON T1.Период = T3.Дата
WHERE
	T1.Период BETWEEN @P_DateTimePeriodBegin AND @P_DateTimePeriodEnd --begin +2
	AND T1.КоличествоЗаказовЗаИнтервалВремени > 0
OPTION (HASH GROUP, 
OPTIMIZE FOR (@P_DateTimePeriodBegin='{0}', @P_DateTimePeriodEnd='{1}')
--,KEEP PLAN, KEEPFIXED PLAN
)
;	
";
		public const string AvailableDateDelivery2 =
@"
WITH Geozones AS (
	SELECT DISTINCT
		T1.Геозона
	FROM #Temp_PlanningGroup T1 WITH(NOLOCK)
)
SELECT DISTINCT
	T1.Геозона,
	T2._Fld25128 НачалоИнтервала,
	T2._Fld25129 ОкончаниеИнтервала
INTO #Temp_GeozoneIntervals
FROM Geozones T1 WITH(NOLOCK)
INNER JOIN dbo._Reference114_VT25126 T2 WITH(NOLOCK) --инГеографическиеЗоны.ВременныеИнтервалы
ON T1.Геозона = T2._Reference114_IDRRef
;

SELECT
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, T3.НачалоИнтервала) AS NUMERIC(12)
        ),
        T1.Период
    ) ВремяНачала,
    T1.Период,
    T1.ГруппаПланирования,
    T1.Геозона
INTO #Temp_Intervals
FROM #Temp_AllIntervals T1 WITH(NOLOCK)
	INNER JOIN #Temp_GeozoneIntervals T3 WITH(NOLOCK)
	ON T1.Геозона = T3.Геозона
		AND T1.ВремяНачала >= T3.НачалоИнтервала
		AND T1.ВремяНачала < T3.ОкончаниеИнтервала
	INNER JOIN #Temp_PlanningGroup T2 WITH(NOLOCK) 
	ON (T1.ГруппаПланирования = T2.ГруппаПланирования)
		AND (T3.НачалоИнтервала >= T2.ВремяДоступностиНаСегодня)
		AND (T2.СтопНаСегодня = 0)
WHERE
    T1.Период = @P_DateTimePeriodBegin
GROUP BY 
	T3.НачалоИнтервала,
	T1.Период,
	T1.ГруппаПланирования,
	T1.Геозона

UNION ALL 

SELECT
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, T3.НачалоИнтервала) AS NUMERIC(12)
        ),
        T1.Период
    ),
    T1.Период,
    T1.ГруппаПланирования,
    T1.Геозона
FROM #Temp_AllIntervals T1 WITH(NOLOCK)
	INNER JOIN #Temp_GeozoneIntervals T3 WITH(NOLOCK)
	ON T1.Геозона = T3.Геозона
		AND T1.ВремяНачала >= T3.НачалоИнтервала
		AND T1.ВремяНачала < T3.ОкончаниеИнтервала
	INNER JOIN #Temp_PlanningGroup T2 WITH(NOLOCK) 
	ON (T1.ГруппаПланирования = T2.ГруппаПланирования)
		AND (T3.НачалоИнтервала >= T2.ВремяДоступностиНаСегодня)
		AND (T2.СтопНаЗавтра = 0
			OR T3.НачалоИнтервала >= T2.ВремяДоступностиНаЗавтра)
WHERE
    T1.Период = DATEADD(DAY, 1, @P_DateTimePeriodBegin)
GROUP BY 
	T3.НачалоИнтервала,
	T1.Период,
	T1.ГруппаПланирования,
	T1.Геозона

UNION ALL 

SELECT
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, T3.НачалоИнтервала) AS NUMERIC(12)
        ),
        T1.Период
    ),
    T1.Период,
    T1.ГруппаПланирования,
    T1.Геозона
FROM #Temp_AllIntervals T1 WITH(NOLOCK)
	INNER JOIN #Temp_GeozoneIntervals T3 WITH(NOLOCK)
	ON T1.Геозона = T3.Геозона
		AND T1.ВремяНачала >= T3.НачалоИнтервала
		AND T1.ВремяНачала < T3.ОкончаниеИнтервала
WHERE
    T1.Период BETWEEN DATEADD(DAY, 2, @P_DateTimePeriodBegin) AND @P_DateTimePeriodEnd 
GROUP BY 
	T3.НачалоИнтервала,
	T1.Период,
	T1.ГруппаПланирования,
	T1.Геозона
OPTION (KEEP PLAN, KEEPFIXED PLAN)
;

WITH PrepareTime AS (
	SELECT 
		T1.Номенклатура,
		ISNULL(T2.ВремяНаПодготовкуПоставщик, 0) ВремяНаПодготовкуПоставщик,
		ISNULL(T2.ВремяНаПодготовкуПеремещение, 0) ВремяНаПодготовкуПеремещение
	FROM #Temp_Goods T1
		INNER JOIN #Temp_ClosestDatesByGoods T3
		ON T1.Номенклатура = T3.Номенклатура
		LEFT JOIN #Temp_PlanningGroup T2
		ON T3.Склад = T2.Склад
			AND (T1.Габарит = T2.Габарит
			OR (T1.Габарит = 0xAC2CBF86E693F63444670FFEB70264EE AND T2.Габарит = 0xAD3F7F5FC4F15DAD4F693CAF8365EC0D)) -- К и Х
)
SELECT
	T1.Номенклатура,
	T1.Склад,
	CASE
		WHEN T1.ПоступлениеОтПоставщика = 0x01
			THEN DATEADD(SECOND, T2.ВремяНаПодготовкуПоставщик, T1.ДатаДоступности)
		WHEN T1.ПеремещениеМеждуСкладами = 0x01
			THEN DATEADD(SECOND, T2.ВремяНаПодготовкуПеремещение, T1.ДатаДоступности)
        ELSE T1.ДатаДоступности
    END ДатаДоступности
INTO #Temp_ShipmentDates
FROM 
	#Temp_ClosestDatesByGoods T1 WITH(NOLOCK)
	INNER JOIN PrepareTime T2 WITH(NOLOCK)
	ON T1.Номенклатура = T2.Номенклатура
OPTION (KEEP PLAN, KEEPFIXED PLAN)
;

SELECT
    T1.Номенклатура,
    MIN(
        ISNULL(
            T2.ВремяНачала,
			CASE
                WHEN (T1.ДатаДоступности > DATEADD(SECOND, -1, @P_DateTimePeriodEnd)) 
				THEN DATEADD(DAY, 1, CAST(CAST(T1.ДатаДоступности AS DATE) AS DATETIME))
                ELSE DATEADD(DAY, 1, @P_DateTimePeriodEnd)
            END
        )
    ) ДатаКурьерскойДоставки
INTO #Temp_AvailableCourier
FROM
    #Temp_ShipmentDates T1 WITH(NOLOCK)
    LEFT JOIN #Temp_Intervals T2 WITH(NOLOCK)
    ON T2.ВремяНачала >= T1.ДатаДоступности
GROUP BY
	T1.Номенклатура
OPTION (KEEP PLAN, KEEPFIXED PLAN)
;

WITH YourTimeInterval AS (
	SELECT TOP 1 
		DATEDIFF(MINUTE, T2._Fld30390, T2._Fld30391) ИнтервалВВашеВремя
	FROM 
		#Temp_PlanningGroup T1 WITH(NOLOCK)
		INNER JOIN _Reference114_VT30388 T2 WITH(NOLOCK)
		ON T2._Reference114_IDRRef = T1.Геозона 
)
SELECT 
	T1.Артикул Article,
	T1.Код Code,
	T2.ДатаКурьерскойДоставки Courier,
	ISNULL(T3.ИнтервалВВашеВремя, 0) YourTimeInterval
From
	#Temp_Goods T1 WITH(NOLOCK)
	INNER JOIN #Temp_AvailableCourier T2 WITH(NOLOCK)
	ON T1.Номенклатура = T2.Номенклатура
	LEFT OUTER JOIN YourTimeInterval T3
	ON 1 = 1
OPTION (KEEP PLAN, KEEPFIXED PLAN)
		";
	}
}

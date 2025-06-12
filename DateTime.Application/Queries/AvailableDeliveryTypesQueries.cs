namespace DateTimeService.Application.Queries
{
    public class AvailableDeliveryTypesQueries
    {
        public const string GoodsRawCreate = @"
            Create Table #Temp_GoodsRaw   
            (	
	            Article nvarchar(20), 
	            Code nvarchar(20), 
                Quantity int 
            )
            ;";

        public const string GoodsRawInsert = @"
            INSERT INTO 
	            #Temp_GoodsRaw ( 
		            Article, Code, Quantity 
	            )
            VALUES
	            {0}
	        OPTION (KEEP PLAN, KEEPFIXED PLAN)
            ;";

        public const string AvailableDelivery = @"Select
	IsNull(ГеозонаСклады._Fld23372RRef, Геозона._Fld23104RRef) As СкладСсылка,
	ЗоныДоставки._ParentIDRRef As ЗонаДоставкиРодительСсылка,
	ЗоныДоставкиРодитель._Description As ЗонаДоставкиРодительНаименование,
    ЗоныДоставки._Fld31473 As КоэффициентЗоныДоставки,
	Геозона._IDRRef As Геозона
Into #Temp_GeoData
From dbo._Reference114 Геозона With (NOLOCK)
	Inner Join _Reference114_VT23370 ГеозонаСклады With (NOLOCK)
	On ГеозонаСклады._Reference114_IDRRef = Геозона._IDRRef
	Inner Join _Reference99 ЗоныДоставки With (NOLOCK)
	On Геозона._Fld2847RRef = ЗоныДоставки._IDRRef
	Inner Join _Reference99 ЗоныДоставкиРодитель With (NOLOCK)
	On ЗоныДоставки._ParentIDRRef = ЗоныДоставкиРодитель._IDRRef
Where Геозона._IDRRef In (
	Select Top 1
		РасстоянияАВ._Fld26708RRef As Геозона --геозона из рс векРасстоянияАВ
	From (Select
			РасстоянияАВ._Fld25549 As КодГорода,
			MAX(РасстоянияАВ._Period) As MAXPERIOD_ 
		From dbo._InfoRg21711 РасстоянияАВ With (NOLOCK)
		Where РасстоянияАВ._Fld26708RRef <> 0x00 
			And РасстоянияАВ._Fld25549 = @P_CityCode
		Group By РасстоянияАВ._Fld25549) T2
		Inner Join dbo._InfoRg21711 РасстоянияАВ With (NOLOCK)
		On T2.КодГорода = РасстоянияАВ._Fld25549 
			And T2.MAXPERIOD_ = РасстоянияАВ._Period)
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select 
	_IDRRef As СкладСсылка
Into #Temp_PickupPoints
From dbo._Reference226 Склады 
Where Склады._Fld19544 in({6}) -- пункты самовывоза pickupPointsString
And @P_IsPickup = 1
OPTION (KEEP PLAN, KEEPFIXED PLAN);

/*Создание таблицы товаров и ее наполнение данными из БД*/
Select 
	Номенклатура._IDRRef As НоменклатураСсылка,
	Упаковки._IDRRef As УпаковкаСсылка,
    Номенклатура._Fld21822RRef As ТНВЭДСсылка,
    Номенклатура._Fld3515RRef As ТоварнаяКатегорияСсылка,
    0x00000000000000000000000000000000 As Склад,
	Sum(T1.quantity) As Количество	
Into #Temp_Goods
From 
	#Temp_GoodsRaw T1
	Inner Join 	dbo._Reference149 Номенклатура With (NOLOCK) 
		On T1.code IS NULL 
		And T1.Article = Номенклатура._Fld3480
	Inner Join dbo._Reference256 Упаковки With (NOLOCK)
		On Упаковки._OwnerID_TYPE = 0x08  
		And Упаковки.[_OwnerID_RTRef] = 0x00000095
		And Номенклатура._IDRRef = Упаковки._OwnerID_RRRef		
		And Упаковки._Fld6003RRef = Номенклатура._Fld3489RRef
		And Упаковки._Marked = 0x00
Group By 
	Номенклатура._IDRRef,
	Упаковки._IDRRef,
    Номенклатура._Fld21822RRef,
    Номенклатура._Fld3515RRef
Union All
Select 
	Номенклатура._IDRRef,
	Упаковки._IDRRef,
    Номенклатура._Fld21822RRef,
    Номенклатура._Fld3515RRef,
    0x00000000000000000000000000000000,
	Sum(T1.quantity)	
From 
	#Temp_GoodsRaw T1
	Inner Join 	dbo._Reference149 Номенклатура With (NOLOCK) 
		ON T1.code IS NOT NULL 
		And T1.code = Номенклатура._Code
	Inner Join dbo._Reference256 Упаковки With (NOLOCK)
		On Упаковки._OwnerID_TYPE = 0x08  
		And Упаковки.[_OwnerID_RTRef] = 0x00000095
		And Номенклатура._IDRRef = Упаковки._OwnerID_RRRef		
		And Упаковки._Fld6003RRef = Номенклатура._Fld3489RRef
		And Упаковки._Marked = 0x00
Group By 
	Номенклатура._IDRRef,
	Упаковки._IDRRef,
    Номенклатура._Fld21822RRef,
    Номенклатура._Fld3515RRef
OPTION (KEEP PLAN, KEEPFIXED PLAN);
/*Конец товаров*/

--маркируемые коды ТН ВЭД
Select Distinct
	T1.КодТНВЭД As КодТНВЭД
Into #Temp_MarkedCodes
From (Select
	ПрослеживаемыеКодыТНВЭД._Fld27184RRef As КодТНВЭД
	From (Select
			ПрослеживаемыеКодыТНВЭД._Fld27184RRef As КодТНВЭД,
			MAX(ПрослеживаемыеКодыТНВЭД._Period) As MAXPERIOD_
		From dbo._InfoRg27183 ПрослеживаемыеКодыТНВЭД
			Inner Join #Temp_Goods T5 With(NOLOCK)
			On (ПрослеживаемыеКодыТНВЭД._Fld27184RRef = T5.ТНВЭДСсылка)
		Group By ПрослеживаемыеКодыТНВЭД._Fld27184RRef) T2
	Inner Join dbo._InfoRg27183 ПрослеживаемыеКодыТНВЭД
	On T2.КодТНВЭД = ПрослеживаемыеКодыТНВЭД._Fld27184RRef 
	And T2.MAXPERIOD_ = ПрослеживаемыеКодыТНВЭД._Period 
	And ПрослеживаемыеКодыТНВЭД._Fld28120 = 0x01) T1;

/*Размеры корзины в целом для расчета габаритов*/
Select
	CAST(SUM((Упаковки._Fld6000 * T1.Количество)) As NUMERIC(36, 6)) As Вес,
	CAST(SUM((Упаковки._Fld6006 * T1.Количество)) As NUMERIC(38, 8)) As Объем,
	MAX(Упаковки._Fld6001) As Высота,
	MAX(Упаковки._Fld6002) As Глубина,
	MAX(Упаковки._Fld6009) As Ширина,
	0x00000000000000000000000000000000 As Габарит
Into #Temp_Size
From #Temp_Goods T1 With(NOLOCK)
	Inner Join dbo._Reference256 Упаковки With(NOLOCK) 
	On (Упаковки._IDRRef = T1.УпаковкаСсылка) 
	And (T1.УпаковкаСсылка <> 0x00000000000000000000000000000000)
OPTION (KEEP PLAN, KEEPFIXED PLAN);

/*Габарит корзины общий*/
Select
    Top 1 Case
        When (
            ISNULL(
                T1.Габарит,
                0x00000000000000000000000000000000
            ) <> 0x00000000000000000000000000000000
        ) Then T1.Габарит
        When (T4._Fld21339 > 0)
        And (T1.Вес >= T4._Fld21339)
        And (T5._Fld21337 > 0)
        And (T1.Объем >= T5._Fld21337) 
			Then 0xAD3F7F5FC4F15DAD4F693CAF8365EC0D --хбт в кбт
        When (T2._Fld21168 > 0)
        And (T1.Вес >= T2._Fld21168) 
			Then 0xAD3F7F5FC4F15DAD4F693CAF8365EC0D --кбт
        When (T3._Fld21166 > 0)
        And (T1.Объем >= T3._Fld21166) 
			Then 0xAD3F7F5FC4F15DAD4F693CAF8365EC0D --кбт
        When (T6._Fld21580 > 0)
        And (T1.Высота > 0)
        And (T1.Глубина > 0)
        And (T1.Ширина >0) 
			Then Case
				When (T1.Высота >= T6._Fld21580) Or (T1.Глубина >= T6._Fld21580) Or (T1.Ширина >= T6._Fld21580) 
					Then 0xAD3F7F5FC4F15DAD4F693CAF8365EC0D --кбт
				Else 0x8AB421D483ABE88A4C4C9928262FFB0D --мбт
			End
        Else 0x8AB421D483ABE88A4C4C9928262FFB0D --мбт
    End As Габарит
Into #Temp_Dimensions
From
    #Temp_Size T1 With(NOLOCK)
    Inner Join dbo._Const21167 T2 On 1 = 1
    Inner Join dbo._Const21165 T3 On 1 = 1
    Inner Join dbo._Const21338 T4 On 1 = 1
    Inner Join dbo._Const21336 T5 On 1 = 1
    Inner Join dbo._Const21579 T6 On 1 = 1
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select
    COUNT_BIG(T1.НоменклатураСсылка) As КоличествоСтрок,
    T1.НоменклатураСсылка As НоменклатураСсылка,
    T2._Fld6000 * T1.Количество As Вес,
    T2._Fld6006 * T1.Количество As Объем,
	SUM(Case 
		When MarkedCodes.КодТНВЭД IS NOT NULL And T1.Количество >= 4
			Then T1.Количество * T3.ДополнительноеВремяМаркируемыеТовары
		Else 0
	End) As УсловиеПоМаркируемымТоварам,		
	Case
        When (
            (T2._Fld6006 > 0.8)
            Or (T2._Fld6002 > 1.85)
            Or (T2._Fld6001 > 1.85)
            Or (T2._Fld6009 > 1.85)
        )
        And (T2._Fld6000 < 120.0)
        And (T2._Fld6000 >= 50.0) Then (T3.УсловиеЭтажМасса1 * @P_Floor)
        When (
            (T2._Fld6006 > 0.8)
            Or (T2._Fld6002 > 1.85)
            Or (T2._Fld6001 > 1.85)
            Or (T2._Fld6009 > 1.85)
        )
        And (T2._Fld6000 >= 120.0) Then (T3.УсловиеЭтажМасса2 * @P_Floor)
        When (
            (T2._Fld6006 > 0.8)
            Or (T2._Fld6002 > 1.85)
            Or (T2._Fld6001 > 1.85)
            Or (T2._Fld6009 > 1.85)
        )
        And (T2._Fld6000 < 5.0) Then (T3.УсловиеЭтажМасса01 * @P_Floor)
        When (
            (T2._Fld6006 > 0.8)
            Or (T2._Fld6002 > 1.85)
            Or (T2._Fld6001 > 1.85)
            Or (T2._Fld6009 > 1.85)
        )
        And (T2._Fld6000 >= 5.0)
        And (T2._Fld6000 < 50.0) Then (T3.УсловиеЭтажМасса02 * @P_Floor)
        Else 0.0
    End As УсловиеЭтажМассаПоТоварам
Into #Temp_Weight
From
    #Temp_Goods T1 With(NOLOCK)
    Left Outer Join dbo._Reference256 T2 With (NOLOCK) 
	On 0x08 = T2._OwnerID_TYPE
    And 0x00000095 = T2._OwnerID_RTRef
    And T1.УпаковкаСсылка = T2._IDRRef
    Inner Join (
        Select
            ПараметрыРасчетаВремениВыполненияРаспоряжения._Fld24101 As УсловиеЭтажМасса1,
            ПараметрыРасчетаВремениВыполненияРаспоряжения._Fld24102 As УсловиеЭтажМасса2,
            ПараметрыРасчетаВремениВыполненияРаспоряжения._Fld26615 As УсловиеЭтажМасса01,
			ПараметрыРасчетаВремениВыполненияРаспоряжения._Fld30450 As ДополнительноеВремяМаркируемыеТовары,
			ПараметрыРасчетаВремениВыполненияРаспоряжения._Fld26616 As УсловиеЭтажМасса02
        From
            (
                Select
                    MAX(ПараметрыРасчетаВремениВыполненияРаспоряжения._Period) As MAXPERIOD_
                From
                    dbo._InfoRg24088 ПараметрыРасчетаВремениВыполненияРаспоряжения
            ) T4
            Inner Join dbo._InfoRg24088 ПараметрыРасчетаВремениВыполненияРаспоряжения 
			On T4.MAXPERIOD_ = ПараметрыРасчетаВремениВыполненияРаспоряжения._Period
    ) T3 On 1 = 1
	Left Outer Join #Temp_MarkedCodes As MarkedCodes
	On MarkedCodes.КодТНВЭД = T1.ТНВЭДСсылка
Group By
    T1.НоменклатураСсылка,
    T2._Fld6000 * T1.Количество,
    T2._Fld6006 * T1.Количество,
    Case
        When (
            (T2._Fld6006 > 0.8)
            Or (T2._Fld6002 > 1.85)
            Or (T2._Fld6001 > 1.85)
            Or (T2._Fld6009 > 1.85)
        )
        And (T2._Fld6000 < 120.0)
        And (T2._Fld6000 >= 50.0) Then (T3.УсловиеЭтажМасса1 * @P_Floor)
        When (
            (T2._Fld6006 > 0.8)
            Or (T2._Fld6002 > 1.85)
            Or (T2._Fld6001 > 1.85)
            Or (T2._Fld6009 > 1.85)
        )
        And (T2._Fld6000 >= 120.0) Then (T3.УсловиеЭтажМасса2 * @P_Floor)
        When (
            (T2._Fld6006 > 0.8)
            Or (T2._Fld6002 > 1.85)
            Or (T2._Fld6001 > 1.85)
            Or (T2._Fld6009 > 1.85)
        )
        And (T2._Fld6000 < 5.0) Then (T3.УсловиеЭтажМасса01 * @P_Floor)
        When (
            (T2._Fld6006 > 0.8)
            Or (T2._Fld6002 > 1.85)
            Or (T2._Fld6001 > 1.85)
            Or (T2._Fld6009 > 1.85)
        )
        And (T2._Fld6000 >= 5.0)
        And (T2._Fld6000 < 50.0) Then (T3.УсловиеЭтажМасса02 * @P_Floor)
        Else 0.0
    End
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select Distinct
    Case
        When (IsNull(T2.Габарит,0x8AB421D483ABE88A4C4C9928262FFB0D) = 0x8AB421D483ABE88A4C4C9928262FFB0D) 
			Then 7 --мбт
        Else 14
    End As УсловиеГабариты,
    0 As УсловиеСпособОплаты,
    T1.КоэффициентЗоныДоставки,
    Case
        When (T1.ЗонаДоставкиРодительНаименование LIKE '%Минск%') --наименование зоны доставки
        And (IsNull(T2.Габарит,0x8AB421D483ABE88A4C4C9928262FFB0D) = 0x8AB421D483ABE88A4C4C9928262FFB0D) 
			Then T3.ДополнительноеВремяМБТ --мбт
        When (T1.ЗонаДоставкиРодительНаименование LIKE '%Минск%')
        And (IsNull(T2.Габарит,0x8AB421D483ABE88A4C4C9928262FFB0D) = 0xAD3F7F5FC4F15DAD4F693CAF8365EC0D) 
			Then T3.ДополнительноеВремяКБТ --кбт
        Else 0
    End As УсловиеМинскЧас
Into #Temp_TimeByOrders
From
    #Temp_GeoData T1 WITH(NOLOCK)
	Left Join #Temp_Dimensions T2 On 1=1
    Inner Join (
        Select
            ПараметрыРасчетаВремениВыполненияРаспоряжения._Fld24103 As УсловиеКредитРассрочкаЗаявка,
            ПараметрыРасчетаВремениВыполненияРаспоряжения._Fld24091 As ДополнительноеВремяМБТ,
            ПараметрыРасчетаВремениВыполненияРаспоряжения._Fld24092 As ДополнительноеВремяКБТ
        From
            (
                Select
                    MAX(ПараметрыРасчетаВремениВыполненияРаспоряжения._Period) As MAXPERIOD_
                From
                    dbo._InfoRg24088 ПараметрыРасчетаВремениВыполненияРаспоряжения
            ) T3
            Inner Join dbo._InfoRg24088 ПараметрыРасчетаВремениВыполненияРаспоряжения 
			On T3.MAXPERIOD_ = ПараметрыРасчетаВремениВыполненияРаспоряжения._Period
    ) T3 On 1 = 1
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select 
	SUM(T1.КоличествоСтрок) AS КоличествоСтрок,
	SUM(Case when T1.УсловиеЭтажМассаПоТоварам = 0 then T1.Вес else 0 end) AS Вес,
	SUM(Case when T1.УсловиеЭтажМассаПоТоварам = 0 then T1.Объем else 0 end) AS Объем,
	SUM(T1.УсловиеЭтажМассаПоТоварам) AS УсловиеЭтажМассаОбщ,
	SUM(T1.УсловиеПоМаркируемымТоварам) AS УсловиеПоМаркируемымТоварам
Into #Temp_TotalWeight
From 
	#Temp_Weight T1
OPTION (KEEP PLAN, KEEPFIXED PLAN);

SELECT
    T2.ВремяНаОднуПозицию * T1.КоличествоСтрок AS УсловиеКоличествоСтрок,
    CASE
        WHEN T1.Объем = 0
			AND T1.Вес = 0 THEN 0
        WHEN T1.Объем < 0.8
			AND T1.Вес < 5.0 THEN T2.УсловиеОбъемВес1
        WHEN T1.Объем < 0.8
			AND T1.Вес >= 5.0
			AND T1.Вес < 20.0 THEN T2.УсловиеОбъемВес2
        WHEN (T1.Объем) < 0.8
			AND T1.Вес >= 20.0
			AND T1.Вес < 65.0 THEN T2.УсловиеОбъемВес3
        WHEN T1.Объем < 0.8
			AND T1.Вес >= 65.0
			AND T1.Вес < 120.0 THEN T2.УсловиеОбъемВес4
        WHEN T1.Объем < 0.8
			AND T1.Вес >= 120.0
			AND T1.Вес < 250.0 THEN T2.УсловиеОбъемВес5
        WHEN T1.Объем < 0.8
			AND T1.Вес >= 250.0
			AND T1.Вес < 400.0 THEN T2.УсловиеОбъемВес5_1
        WHEN T1.Объем < 0.8
			AND T1.Вес >= 400.0 THEN T2.УсловиеОбъемВес5_2
        WHEN T1.Объем >= 0.8
			AND T1.Вес < 120.0 THEN T2.УсловиеОбъемВес6
        WHEN T1.Объем >= 0.8
			AND T1.Вес >= 120.0
			AND T1.Вес < 250.0 THEN T2.УсловиеОбъемВес7
        WHEN T1.Объем >= 0.8
			AND T1.Вес >= 250.0
			AND T1.Вес < 600.0 THEN T2.УсловиеОбъемВес8
        WHEN T1.Объем >= 0.8
			AND T1.Вес >= 600.0 THEN T2.УсловиеОбъемВес9
    END As УсловиеВесОбъем,
    T2.МинимальноеВремя As МинимальноеВремя,
    T1.УсловиеПоМаркируемымТоварам AS УсловиеПоМаркируемымТоварам,
    T1.УсловиеЭтажМассаОбщ As УсловиеЭтажМассаОбщ
INTO #Temp_Time1
FROM
    #Temp_TotalWeight T1 WITH(NOLOCK)
    Inner Join (
        Select
            ПараметрыРасчетаВремениВыполненияРаспоряжения._Fld24090 As ВремяНаОднуПозицию,
            ПараметрыРасчетаВремениВыполненияРаспоряжения._Fld24094 As УсловиеОбъемВес1,
            ПараметрыРасчетаВремениВыполненияРаспоряжения._Fld24095 As УсловиеОбъемВес2,
            ПараметрыРасчетаВремениВыполненияРаспоряжения._Fld24096 As УсловиеОбъемВес3,
            ПараметрыРасчетаВремениВыполненияРаспоряжения._Fld24097 As УсловиеОбъемВес4,
            ПараметрыРасчетаВремениВыполненияРаспоряжения._Fld24098 As УсловиеОбъемВес5,
            ПараметрыРасчетаВремениВыполненияРаспоряжения._Fld26611 As УсловиеОбъемВес5_1,
            ПараметрыРасчетаВремениВыполненияРаспоряжения._Fld26612 As УсловиеОбъемВес5_2,
            ПараметрыРасчетаВремениВыполненияРаспоряжения._Fld24099 As УсловиеОбъемВес6,
            ПараметрыРасчетаВремениВыполненияРаспоряжения._Fld24100 As УсловиеОбъемВес7,
            ПараметрыРасчетаВремениВыполненияРаспоряжения._Fld26613 As УсловиеОбъемВес8,
            ПараметрыРасчетаВремениВыполненияРаспоряжения._Fld26614 As УсловиеОбъемВес9,
            ПараметрыРасчетаВремениВыполненияРаспоряжения._Fld24089 As МинимальноеВремя
        From
            (
                Select
                    MAX(ПараметрыРасчетаВремениВыполненияРаспоряжения._Period) As MAXPERIOD_
                From
                    dbo._InfoRg24088 ПараметрыРасчетаВремениВыполненияРаспоряжения
            ) T3
            Inner Join dbo._InfoRg24088 ПараметрыРасчетаВремениВыполненияРаспоряжения 
			On T3.MAXPERIOD_ = ПараметрыРасчетаВремениВыполненияРаспоряжения._Period
    ) T2 ON 1 = 1
OPTION (KEEP PLAN, KEEPFIXED PLAN);

/*Время обслуживания началось выше и тут итоговая цифра*/
Select
    (ISNULL(T2.МинимальноеВремя, 0) 
	+ ISNULL(T2.УсловиеКоличествоСтрок, 0) 
	+ ISNULL(T1.УсловиеМинскЧас, 0) 
	+ ISNULL(T2.УсловиеЭтажМассаОбщ, 0) 
	+ ISNULL(T2.УсловиеВесОбъем, 0) 
	+ ISNULL(T1.УсловиеСпособОплаты, 0) 
	+ ISNULL(T2.УсловиеПоМаркируемымТоварам, 0)) * T1.КоэффициентЗоныДоставки
	As ВремяВыполнения
Into #Temp_TimeService
From
    #Temp_TimeByOrders T1 With(NOLOCK)
    Left Outer Join #Temp_Time1 T2 WITH(NOLOCK)
    On 1 = 1
OPTION (KEEP PLAN, KEEPFIXED PLAN);

/*Группа планирования*/
Select ГруппыПланирования._IDRRef As ГруппаПланирования,
	ГруппыПланирования._Fld23302RRef As Склад,
	Case When @P_YourTimeDelivery = 1
		Then ГруппыПланирования._Fld30397
		Else ГруппыПланирования._Fld25137
	End As ВремяДоступностиНаСегодня,
	ГруппыПланирования._Fld25138 As ВремяСтопаСегодня,
	ГруппыПланирования._Fld25139 As ВремяДоступностиНаЗавтра,
	ГруппыПланирования._Fld25140 As ВремяСтопаЗавтра,
	Case When @P_YourTimeDelivery = 1
		Then ГруппыПланирования._Fld30399
		Else ГруппыПланирования._Fld25519
	End As ГруппаПланированияДобавляемоеВремя,
	1 As Основная,
	ГруппыПланирования._Description
Into #Temp_PlanningGroups
From
dbo._Reference23294 ГруппыПланирования With (NOLOCK)
	Inner Join dbo._Reference23294_VT23309 With (NOLOCK)
		On ГруппыПланирования._IDRRef = _Reference23294_VT23309._Reference23294_IDRRef
		And _Reference23294_VT23309._Fld23311RRef In (Select ЗонаДоставкиРодительСсылка From #Temp_GeoData)
	And ГруппыПланирования._Fld25141 = 0x01--участвует в расчете мощности
	And ГруппыПланирования._Fld23301RRef In (Select Габарит From #Temp_Dimensions With (NOLOCK))  --габариты
	And ГруппыПланирования._Marked = 0x00
    And @P_IsDelivery = 1
	And Case When @P_YourTimeDelivery = 1
		Then
			ГруппыПланирования._Fld30395 --участвует в доставке в ваше время
		Else 0x01
		End = 0x01		
Union All
Select 
	ПодчиненнаяГП._IDRRef As ГруппаПланирования,
	ГруппыПланирования._Fld23302RRef As Склад,
	Case When @P_YourTimeDelivery = 1
		Then ПодчиненнаяГП._Fld30397
		Else ПодчиненнаяГП._Fld25137
	End As ВремяДоступностиНаСегодня,
	ПодчиненнаяГП._Fld25138 As ВремяСтопаСегодня,
	ПодчиненнаяГП._Fld25139 As ВремяДоступностиНаЗавтра,
	ПодчиненнаяГП._Fld25140 As ВремяСтопаЗавтра,
	Case When @P_YourTimeDelivery = 1
		Then ПодчиненнаяГП._Fld30399
		Else ПодчиненнаяГП._Fld25519
	End As ГруппаПланированияДобавляемоеВремя,
	0,
	ПодчиненнаяГП._Description
From
	dbo._Reference23294 ГруппыПланирования With (NOLOCK)
	Inner Join dbo._Reference23294_VT23309	With (NOLOCK)	
		on ГруппыПланирования._IDRRef = _Reference23294_VT23309._Reference23294_IDRRef
		And _Reference23294_VT23309._Fld23311RRef In (Select ЗонаДоставкиРодительСсылка From #Temp_GeoData)
	Inner Join dbo._Reference23294 ПодчиненнаяГП
			On ГруппыПланирования._Fld26526RRef = ПодчиненнаяГП._IDRRef
                And @P_IsDelivery = 1
				And Case When @P_YourTimeDelivery = 1
					Then
						ПодчиненнаяГП._Fld30395 --участвует в доставке в ваше время
					Else 0x01
					End = 0x01		
Where 
	ГруппыПланирования._Fld25141 = 0x01--участвует в расчете мощности
	And ГруппыПланирования._Marked = 0x00
OPTION (KEEP PLAN, KEEPFIXED PLAN);

/*Отсюда начинается процесс получения оптимальной даты отгрузки*/
With Temp_ExchangeRates As (
Select
	КурсыВалют._Period As Период,
	КурсыВалют._Fld14558RRef As Валюта,
	КурсыВалют._Fld14559 As Курс,
	КурсыВалют._Fld14560 As Кратность
From _InfoRgSL26678 КурсыВалют With (NOLOCK)
	)
Select
    РезервированиеИтоги._Fld21408RRef As НоменклатураСсылка,
    РезервированиеИтоги._Fld21410_TYPE As Источник_TYPE,
	РезервированиеИтоги._Fld21410_RTRef As Источник_RTRef,
	РезервированиеИтоги._Fld21410_RRRef As Источник_RRRef,
	Резервирование._Fld21410_TYPE As Регистратор_TYPE,
    Резервирование._Fld21410_RTRef As Регистратор_RTRef,
    Резервирование._Fld21410_RRRef As Регистратор_RRRef,
    РезервированиеИтоги._Fld23568RRef As СкладИсточника,
    РезервированиеИтоги._Fld21424 As ДатаСобытия,
    SUM(РезервированиеИтоги._Fld21411) - SUM(РезервированиеИтоги._Fld21412) As Количество
Into #Temp_Remains
From
    dbo._AccumRgT21444 РезервированиеИтоги With (NOLOCK)
	Left Join _AccumRg21407 Резервирование With (NOLOCK)
		Inner Join Temp_ExchangeRates With (NOLOCK)
			On Резервирование._Fld21443RRef = Temp_ExchangeRates.Валюта 
		On РезервированиеИтоги._Fld21408RRef = Резервирование._Fld21408RRef
		And РезервированиеИтоги._Fld21410_RTRef = 0x00000153
		And Резервирование._Fld21410_RTRef = 0x00000153 --Цены.Регистратор ССЫЛКА Документ.мегапрайсРегистрацияПрайса
		And РезервированиеИтоги._Fld21410_RRRef = Резервирование._Fld21410_RRRef
        And Резервирование._Fld30969 = 0x00 -- ОтказПоФильтруЦен = Ложь
        --And (Резервирование._Fld21982 <> 0 
		--And Резервирование._Fld21442 * Temp_ExchangeRates.Курс / Temp_ExchangeRates.Кратность >= Резервирование._Fld21982 
			--Or Резервирование._Fld21411 >= Резервирование._Fld21616)
		And Резервирование._Fld21408RRef In (Select
                НоменклатураСсылка
            From
                #Temp_Goods)
Where
    РезервированиеИтоги._Period = '5999-11-01 00:00:00'
    And (
        (
            (РезервированиеИтоги._Fld21424 = '2001-01-01 00:00:00')
            Or (Cast(РезервированиеИтоги._Fld21424 As datetime) >= @P_DateTimeNow)
        )
        And РезервированиеИтоги._Fld21408RRef In (
            Select
                Goods.НоменклатураСсылка
            From
                #Temp_Goods Goods WITH(NOLOCK))) And (РезервированиеИтоги._Fld21412 <> 0 Or РезервированиеИтоги._Fld21411 <> 0)
Group By
    РезервированиеИтоги._Fld21408RRef,
    РезервированиеИтоги._Fld21410_TYPE,
    РезервированиеИтоги._Fld21410_RTRef,
    РезервированиеИтоги._Fld21410_RRRef,
	Резервирование._Fld21410_TYPE,
    Резервирование._Fld21410_RTRef,
    Резервирование._Fld21410_RRRef,
    РезервированиеИтоги._Fld23568RRef,
    РезервированиеИтоги._Fld21424
Having
    (SUM(РезервированиеИтоги._Fld21412) <> 0.0
		Or SUM(РезервированиеИтоги._Fld21411) <> 0.0)
	And SUM(РезервированиеИтоги._Fld21411) - SUM(РезервированиеИтоги._Fld21412) > 0.0
OPTION (HASH GROUP, OPTIMIZE FOR (@P_DateTimeNow='{0}'),KEEP PLAN, KEEPFIXED PLAN);

With SourceWarehouses AS
(
--Select Distinct
--	T2.СкладИсточника As СкладИсточника
--From
--	#Temp_Remains T2 WITH(NOLOCK)
Select 
	Subquery.СкладИсточника
From
	(Select 
		T2.СкладИсточника As СкладИсточника,
		T2.НоменклатураСсылка, 
		ROW_NUMBER() OVER(
		PARTITION BY НоменклатураСсылка
		ORDER BY СкладИсточника) AS rownum
From
	#Temp_Remains T2 WITH(NOLOCK)) As Subquery
Where Subquery.rownum <= 10
)
Select Distinct
    ПрогнозныеДатыПоставокНаСклады._Fld23831RRef As СкладИсточника,
    ПрогнозныеДатыПоставокНаСклады._Fld23832 As ДатаСобытия,
    ПрогнозныеДатыПоставокНаСклады._Fld23834 As ДатаПрибытия,
    ПрогнозныеДатыПоставокНаСклады._Fld23833RRef As СкладНазначения
Into #Temp_WarehouseDates
From
    dbo._InfoRg23830 ПрогнозныеДатыПоставокНаСклады With (NOLOCK)
	Inner Join #Temp_Remains With (NOLOCK)
	On ПрогнозныеДатыПоставокНаСклады._Fld23831RRef = #Temp_Remains.СкладИсточника
	And ПрогнозныеДатыПоставокНаСклады._Fld23832 = #Temp_Remains.ДатаСобытия
	And ПрогнозныеДатыПоставокНаСклады._Fld23833RRef In (Select СкладСсылка From #Temp_GeoData Union All Select СкладСсылка From #Temp_PickupPoints)
    Inner Join SourceWarehouses 
	On ПрогнозныеДатыПоставокНаСклады._Fld23831RRef = SourceWarehouses.СкладИсточника
OPTION (KEEP PLAN, KEEPFIXED PLAN);

With SourceWarehouses AS
(
--Select Distinct
--	T2.СкладИсточника As СкладИсточника
--From
--	#Temp_Remains T2 WITH(NOLOCK)
Select 
	Subquery.СкладИсточника
From
	(Select 
		T2.СкладИсточника As СкладИсточника,
		T2.НоменклатураСсылка, 
		ROW_NUMBER() OVER(
		PARTITION BY НоменклатураСсылка
		ORDER BY СкладИсточника) AS rownum
From
	#Temp_Remains T2 WITH(NOLOCK)) As Subquery
Where Subquery.rownum <= 10
)
Select
	ПрогнозныеДатыПоставокНаСклады._Fld23831RRef As СкладИсточника,
	ПрогнозныеДатыПоставокНаСклады._Fld23833RRef As СкладНазначения,
	MIN(ПрогнозныеДатыПоставокНаСклады._Fld23834) As ДатаПрибытия 
Into #Temp_MinimumWarehouseDates
From
    dbo._InfoRg23830 ПрогнозныеДатыПоставокНаСклады With (NOLOCK{5}) 
    Inner Join SourceWarehouses 
	On ПрогнозныеДатыПоставокНаСклады._Fld23831RRef = SourceWarehouses.СкладИсточника	
Where
	ПрогнозныеДатыПоставокНаСклады._Fld23833RRef In (Select СкладСсылка From #Temp_GeoData Union All Select СкладСсылка From #Temp_PickupPoints)
		And	ПрогнозныеДатыПоставокНаСклады._Fld23832 Between @P_DateTimeNow And DateAdd(DAY,6,@P_DateTimeNow)
Group By 
	ПрогнозныеДатыПоставокНаСклады._Fld23831RRef,
	ПрогнозныеДатыПоставокНаСклады._Fld23833RRef
OPTION (HASH GROUP, KEEP PLAN, KEEPFIXED PLAN);

Select
    T1.НоменклатураСсылка,
    T1.Количество,
    T1.Источник_TYPE,
    T1.Источник_RTRef,
    T1.Источник_RRRef,
    T1.СкладИсточника,
    T1.ДатаСобытия,
    ISNULL(T3.ДатаПрибытия, T2.ДатаПрибытия) As ДатаДоступности,
    1 As ТипИсточника,
    1 As ЭтоСклад,
    ISNULL(T3.СкладНазначения, T2.СкладНазначения) As СкладНазначения
Into #Temp_Sources
From
    #Temp_Remains T1 WITH(NOLOCK)
    Left Outer Join #Temp_WarehouseDates T2 WITH(NOLOCK)
    On (T1.СкладИсточника = T2.СкладИсточника)
    And (T1.ДатаСобытия = T2.ДатаСобытия)
    Left Outer Join #Temp_MinimumWarehouseDates T3 WITH(NOLOCK)
    On (T1.СкладИсточника = T3.СкладИсточника)
    And (T1.ДатаСобытия = '2001-01-01 00:00:00')
Where
    T1.Количество > 0 And
    T1.Источник_RTRef = 0x000000E2 Or T1.Источник_RTRef = 0x00000150 --склад, корректировка регистров

Union All

Select
    T4.НоменклатураСсылка,
    T4.Количество,
    T4.Источник_TYPE,
    T4.Источник_RTRef,
    T4.Источник_RRRef,
    T4.СкладИсточника,
    T4.ДатаСобытия,
	DATEADD(SECOND, DATEDIFF(SECOND, @P_EmptyDate, IsNull(#Temp_PlanningGroups.ГруппаПланированияДобавляемоеВремя, @P_EmptyDate)), T5.ДатаПрибытия),
    2,
    1,
    T5.СкладНазначения
From
    #Temp_Remains T4 WITH(NOLOCK)
    Inner Join #Temp_WarehouseDates T5 WITH(NOLOCK)
    On (T4.СкладИсточника = T5.СкладИсточника)
    And (T4.ДатаСобытия = T5.ДатаСобытия)
	Left Join #Temp_PlanningGroups 
	On T5.СкладНазначения = #Temp_PlanningGroups.Склад 
	And #Temp_PlanningGroups.Основная = 1
Where
    T4.Количество > 0 And
    T4.Источник_RTRef = 0x00000141 --заказ поставщику

Union All

Select
    T6.НоменклатураСсылка,
    T6.Количество,
    T6.Источник_TYPE,
    T6.Источник_RTRef,
    T6.Источник_RRRef,
    T6.СкладИсточника,
    T6.ДатаСобытия,
	DATEADD(SECOND, DATEDIFF(SECOND, @P_EmptyDate, IsNull(#Temp_PlanningGroups.ГруппаПланированияДобавляемоеВремя,@P_EmptyDate)), T7.ДатаПрибытия),
    3,
    0,
    T7.СкладНазначения
From
    #Temp_Remains T6 WITH(NOLOCK)
    Inner Join #Temp_WarehouseDates T7 WITH(NOLOCK)
    On (T6.СкладИсточника = T7.СкладИсточника)
    And (T6.ДатаСобытия = T7.ДатаСобытия)
	Left Join #Temp_PlanningGroups With (NOLOCK) 
	On T7.СкладНазначения = #Temp_PlanningGroups.Склад 
	And #Temp_PlanningGroups.Основная = 1
Where
    T6.Количество > 0 
	And NOT T6.Регистратор_RRRef IS NULL
	And T6.Источник_RTRef = 0x00000153 --мегапрайсРегистрацияПрайса
OPTION (KEEP PLAN, KEEPFIXED PLAN);

With TempSourcesGrouped AS
(
Select
	T1.НоменклатураСсылка As НоменклатураСсылка,
	T1.ЭтоСклад,
	Sum(T1.Количество) As Количество,
	T1.ДатаДоступности As ДатаДоступности,
	T1.СкладНазначения As СкладНазначения
From
	#Temp_Sources T1	
Group By
	T1.НоменклатураСсылка,
	T1.ЭтоСклад,
	T1.ДатаДоступности,
	T1.СкладНазначения
)
Select
	Источники1.НоменклатураСсылка As Номенклатура,
	Источники1.СкладНазначения As СкладНазначения,
	Источники1.ДатаДоступности As ДатаДоступности,
	Sum(Источник2.Количество) As Количество
Into #Temp_AvailableGoods
From
	TempSourcesGrouped As Источники1
		Left Join TempSourcesGrouped As Источник2
		On Источники1.НоменклатураСсылка = Источник2.НоменклатураСсылка
		And Источники1.СкладНазначения = Источник2.СкладНазначения
			And Источники1.ДатаДоступности >= Источник2.ДатаДоступности	
Group By
	Источники1.НоменклатураСсылка,
	Источники1.ДатаДоступности,
	Источники1.СкладНазначения
OPTION (KEEP PLAN, KEEPFIXED PLAN);

With Temp_ExchangeRates As (
Select
	T1._Period As Период,
	T1._Fld14558RRef As Валюта,
	T1._Fld14559 As Курс,
	T1._Fld14560 As Кратность
From _InfoRgSL26678 T1 With (NOLOCK)
)
Select
    T1.НоменклатураСсылка,
	T1.Количество,
    T1.Источник_TYPE,
    T1.Источник_RTRef,
    T1.Источник_RRRef,
    T1.СкладИсточника,
    T1.СкладНазначения,
    T1.ДатаСобытия,
    T1.ДатаДоступности,
    CAST(
        (
            CAST(
                (Резервирование._Fld21442 * T3.Курс) As NUMERIC(27, 8)
            ) / T3.Кратность
        ) As NUMERIC(15, 2)
    )  As Цена
Into #Temp_SourcesWithPrices
From
    #Temp_Sources T1 WITH(NOLOCK)
    Inner Join dbo._AccumRg21407 Резервирование WITH(NOLOCK)
    Left Outer Join Temp_ExchangeRates T3 WITH(NOLOCK)
    On (Резервирование._Fld21443RRef = T3.Валюта) 
    On (T1.НоменклатураСсылка = Резервирование._Fld21408RRef)
    And (
        T1.Источник_TYPE = 0x08
        And T1.Источник_RTRef = Резервирование._RecorderTRef
        And T1.Источник_RRRef = Резервирование._RecorderRRef
    )
OPTION (KEEP PLAN, KEEPFIXED PLAN, maxdop 4);

With Temp_SupplyDocs AS
(
Select
    T1.НоменклатураСсылка,
    T1.СкладНазначения,
    T1.ДатаДоступности,
    DATEADD(DAY, {3}, T1.ДатаДоступности) As ДатаДоступностиПлюс, --это параметр КоличествоДнейАнализа
    MIN(T1.Цена) As ЦенаИсточника,
    MIN(T1.Цена / 100.0 * (100 - {4})) As ЦенаИсточникаМинус --это параметр ПроцентДнейАнализа
From
    #Temp_SourcesWithPrices T1 WITH(NOLOCK)
Where
    T1.Цена <> 0
    And T1.Источник_RTRef = 0x00000153    
Group By
    T1.НоменклатураСсылка,
    T1.ДатаДоступности,
    T1.СкладНазначения,
    DATEADD(DAY, {3}, T1.ДатаДоступности)--это параметр КоличествоДнейАнализа
)
Select
    T2.НоменклатураСсылка,
    T2.ДатаДоступности,
    T2.СкладНазначения,
    T2.ДатаДоступностиПлюс,
    T2.ЦенаИсточника,
    T2.ЦенаИсточникаМинус,
    MIN(T1.ДатаДоступности) As ДатаДоступности1,
    MIN(T1.Цена) As Цена1
Into #Temp_BestPriceAfterClosestDate
From
    #Temp_SourcesWithPrices T1 WITH(NOLOCK)
    Inner Hash Join Temp_SupplyDocs T2 WITH(NOLOCK)
    On (T1.НоменклатураСсылка = T2.НоменклатураСсылка)
    And (T1.ДатаДоступности >= T2.ДатаДоступности)
    And (T1.ДатаДоступности <= T2.ДатаДоступностиПлюс)
    And (T1.Цена <= T2.ЦенаИсточникаМинус)
    And (T1.Цена <> 0)
Group By
    T2.НоменклатураСсылка,
    T2.СкладНазначения,
    T2.ДатаДоступностиПлюс,
    T2.ЦенаИсточника,
    T2.ЦенаИсточникаМинус,
    T2.ДатаДоступности
OPTION (HASH GROUP, KEEP PLAN, KEEPFIXED PLAN);

Select
    T1.НоменклатураСсылка,
	T1.Количество,
    T1.Источник_TYPE,
    T1.Источник_RTRef,
    T1.Источник_RRRef,
    T1.СкладИсточника,
    T1.СкладНазначения,
    T1.ДатаСобытия,
    ISNULL(T2.ДатаДоступности1, T1.ДатаДоступности) As ДатаДоступности,
    T1.ТипИсточника
Into #Temp_SourcesCorrectedDate
From
    #Temp_Sources T1 WITH(NOLOCK)
    Left Outer Join #Temp_BestPriceAfterClosestDate T2 WITH(NOLOCK)
    On (T1.НоменклатураСсылка = T2.НоменклатураСсылка)
    And (T1.ДатаДоступности = T2.ДатаДоступности)
    And (T1.СкладНазначения = T2.СкладНазначения)
    And (T1.ТипИсточника = 3)
OPTION (KEEP PLAN, KEEPFIXED PLAN);

With Temp_ClosestDate AS
(
Select
    T1.НоменклатураСсылка,
    T1.СкладНазначения,
    MIN(T1.ДатаДоступности) As ДатаДоступности
From 
    #Temp_Sources T1 WITH(NOLOCK)
Group By 
    T1.НоменклатураСсылка,
    T1.СкладНазначения
)
Select 
	T4.НоменклатураСсылка As НоменклатураСсылка,
	T4.СкладНазначения As СкладНазначения,
	Min(T4.БлижайшаяДата) As БлижайшаяДата
Into #Temp_ClosestDatesByGoods
From 
(Select
    T1.НоменклатураСсылка,
    ISNULL(T3.СкладНазначения, T2.СкладНазначения) As СкладНазначения,
    MIN(ISNULL(T3.ДатаДоступности, T2.ДатаДоступности)) As БлижайшаяДата
From
    #Temp_Goods T1 WITH(NOLOCK)
    Left Outer Join #Temp_SourcesCorrectedDate T2 WITH(NOLOCK)
    On (T1.НоменклатураСсылка = T2.НоменклатураСсылка)
    Left Outer Join (
        -- нам здесь не важна реальная ближайшая дата, главное, есть ли она. иначе этот запрос выбирает огромное количество записей
        Select Top 1
            T4.НоменклатураСсылка,
            T4.ДатаДоступности,
            T4.СкладНазначения,
            T5.ДатаДоступности As БлижайшаяДата
        From
            #Temp_Sources T4 WITH(NOLOCK)
            Left Outer Join Temp_ClosestDate T5 WITH(NOLOCK)
            On (T4.НоменклатураСсылка = T5.НоменклатураСсылка)
            And (T4.СкладНазначения = T5.СкладНазначения)
            And (T4.ТипИсточника = 1 Or T4.ТипИсточника = 4)
        Where T4.ДатаДоступности <= DATEADD(DAY, {3}, T5.ДатаДоступности) --это параметр КоличествоДнейАнализа
            And @P_IsPickup = 0
                Or T4.СкладНазначения In (Select PP.СкладСсылка From #Temp_PickupPoints PP)
    ) T3 On (T1.НоменклатураСсылка = T3.НоменклатураСсылка)
    And (
        T3.ДатаДоступности <= DATEADD(DAY, {3}, T3.БлижайшаяДата) --это параметр КоличествоДнейАнализа
    )
	Where T1.Количество = 1
Group By
    T1.НоменклатураСсылка,
    ISNULL(T3.СкладНазначения, T2.СкладНазначения)

Union All

Select 
	#Temp_Goods.НоменклатураСсылка,
	#Temp_AvailableGoods.СкладНазначения,
	Min(#Temp_AvailableGoods.ДатаДоступности)
From #Temp_Goods With (NOLOCK)
	Left Join #Temp_AvailableGoods With (NOLOCK) 
		On #Temp_Goods.НоменклатураСсылка = #Temp_AvailableGoods.Номенклатура
		And #Temp_Goods.Количество <= #Temp_AvailableGoods.Количество
Where
	#Temp_Goods.Количество > 1
Group By
	#Temp_Goods.НоменклатураСсылка,
	#Temp_AvailableGoods.СкладНазначения) T4
Group By 
	T4.НоменклатураСсылка,
	T4.СкладНазначения
OPTION (HASH GROUP, KEEP PLAN, KEEPFIXED PLAN);

With Temp_CountOfGoods AS
(
Select
    COUNT(DISTINCT T1.НоменклатураСсылка) As CountOfGoods
From 
    #Temp_Goods T1 WITH(NOLOCK)
)
Select Top 1 
	Max(#Temp_ClosestDatesByGoods.БлижайшаяДата) As DateAvailable, 
	СкладНазначения As СкладНазначения
Into #Temp_DateAvailable
From #Temp_ClosestDatesByGoods With (NOLOCK)
	Left Outer Join Temp_CountOfGoods
	ON 1 = 1
Where 
    @P_IsPickup = 0
	Or #Temp_ClosestDatesByGoods.СкладНазначения In (Select СкладСсылка From #Temp_PickupPoints)
Group By СкладНазначения
Having 
	COUNT(DISTINCT #Temp_ClosestDatesByGoods.НоменклатураСсылка) = Min(Temp_CountOfGoods.CountOfGoods)
Order By DateAvailable ASC
OPTION (KEEP PLAN, KEEPFIXED PLAN);
/*Тут закончился процесс оптимальной даты. Склад назначения нужен чтоб потом правильную ГП выбрать*/

/*Интервалы для ПВЗ*/
WITH Tdate(date, СкладНазначения) As (
    /*Это получение списка дат интервалов после даты окончания расчета*/
    Select         
		CAST(CAST(#Temp_DateAvailable.DateAvailable  As DATE) As DATETIME), 		
		#Temp_DateAvailable.СкладНазначения
	From #Temp_DateAvailable
	Where #Temp_DateAvailable.СкладНазначения in (Select СкладСсылка From #Temp_PickupPoints)
    
	Union All

    Select 
        DateAdd(day, 1, Tdate.date),
		#Temp_DateAvailable.СкладНазначения
    From
        Tdate
		Inner Join #Temp_DateAvailable 
		On Tdate.date < DateAdd(DAY, @P_DaysToShow, CAST(CAST(#Temp_DateAvailable.DateAvailable  As DATE) As DATETIME))
		And Tdate.СкладНазначения = #Temp_DateAvailable.СкладНазначения
		And #Temp_DateAvailable.СкладНазначения in (Select СкладСсылка From #Temp_PickupPoints)
)
Select	
	Case 
	When 
		DATEADD(
			SECOND,
			CAST(
				DATEDIFF(SECOND, @P_EmptyDate, isNull(ПВЗИзмененияГрафикаРаботы._Fld27057, ПВЗГрафикРаботы._Fld23617)) As NUMERIC(12)
			),
			date
		) < #Temp_DateAvailable.DateAvailable 
		Then #Temp_DateAvailable.DateAvailable
	Else
		DATEADD(
			SECOND,
			CAST(
				DATEDIFF(SECOND, @P_EmptyDate, isNull(ПВЗИзмененияГрафикаРаботы._Fld27057, ПВЗГрафикРаботы._Fld23617)) As NUMERIC(12)
			),
			date
		)
	End As ВремяНачала,
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, isNull(ПВЗИзмененияГрафикаРаботы._Fld27058, ПВЗГрафикРаботы._Fld23618)) As NUMERIC(12)
        ),
        date
    ) As ВремяОкончания
Into #Temp_AvailablePickUp
From
    #Temp_DateAvailable
		Inner Join Tdate 
		On #Temp_DateAvailable.СкладНазначения = Tdate.СкладНазначения
		Inner Join dbo._Reference226 Склады 
		On Склады._IDRRef = #Temp_DateAvailable.СкладНазначения
			Inner Join _Reference23612 
			On Склады._Fld23620RRef = _Reference23612._IDRRef
				Left Join _Reference23612_VT23613 As ПВЗГрафикРаботы 
				On _Reference23612._IDRRef = _Reference23612_IDRRef
				And (Case When @@DATEFIRST = 1 Then DATEPART(dw, Tdate.date) When DATEPART(dw, Tdate.date) = 1 Then 7 Else DATEPART(dw, Tdate.date) -1 End) = ПВЗГрафикРаботы._Fld23615
		Left Join _Reference23612_VT27054 As ПВЗИзмененияГрафикаРаботы 
				On _Reference23612._IDRRef = ПВЗИзмененияГрафикаРаботы._Reference23612_IDRRef
				And Tdate.date = ПВЗИзмененияГрафикаРаботы._Fld27056
		Where 
			Case 
			When ПВЗИзмененияГрафикаРаботы._Reference23612_IDRRef is not null
				Then ПВЗИзмененияГрафикаРаботы._Fld27059
			When ПВЗГрафикРаботы._Reference23612_IDRRef is not Null 
				Then ПВЗГрафикРаботы._Fld25265 
			Else 0 --не найдено ни графика ни изменения графика  
			End = 0x00  -- не выходной
		And DATEADD(
			SECOND,
			CAST(
            DATEDIFF(SECOND, @P_EmptyDate, isNull(ПВЗИзмененияГрафикаРаботы._Fld27058, ПВЗГрафикРаботы._Fld23618)) As NUMERIC(12)
			),
			Tdate.date) > #Temp_DateAvailable.DateAvailable
OPTION (KEEP PLAN, KEEPFIXED PLAN);
/*Конец интервалов для ПВЗ*/

/*Мощности доставки*/
Select
    CAST(
        SUM(
            Case
                When (МощностиДоставки._RecordKind = 0.0) Then МощностиДоставки._Fld25107
                Else -(МощностиДоставки._Fld25107)
            End
        ) As NUMERIC(16, 3)
    ) As МассаОборот,
    CAST(
        SUM(
            Case
                When (МощностиДоставки._RecordKind = 0.0) Then МощностиДоставки._Fld25108
                Else -(МощностиДоставки._Fld25108)
            End
        ) As NUMERIC(16, 3)
    ) As ОбъемОборот,
    CAST(
        SUM(
            Case
                When (МощностиДоставки._RecordKind = 0.0) Then МощностиДоставки._Fld25201
                Else -(МощностиДоставки._Fld25201)
            End
        ) As NUMERIC(16, 2)
    ) As ВремяНаОбслуживаниеОборот,
	CAST(CAST(МощностиДоставки._Period As DATE) As DATETIME) As Дата
Into #Temp_DeliveryPower
From
    dbo._AccumRg25104 МощностиДоставки With (NOLOCK),
	#Temp_Size With (NOLOCK),
	#Temp_TimeService With (NOLOCK)
Where
    МощностиДоставки._Period >= @P_DateTimePeriodBegin
    And МощностиДоставки._Period <= @P_DateTimePeriodEnd
	And МощностиДоставки._Fld25105RRef In (Select ЗонаДоставкиРодительСсылка From #Temp_GeoData)
Group By
	CAST(CAST(МощностиДоставки._Period As DATE) As DATETIME),
	#Temp_Size.Вес,
	#Temp_Size.Объем,
	#Temp_TimeService.ВремяВыполнения
Having 
	SUM(
            Case
                When (МощностиДоставки._RecordKind = 0.0) Then МощностиДоставки._Fld25107
                Else -(МощностиДоставки._Fld25107)
            End
        ) > #Temp_Size.Вес
	And 
	SUM(
            Case
                When (МощностиДоставки._RecordKind = 0.0) Then МощностиДоставки._Fld25108
                Else -(МощностиДоставки._Fld25108)
            End
        ) > #Temp_Size.Объем
	And 
	SUM(
            Case
                When (МощностиДоставки._RecordKind = 0.0) Then МощностиДоставки._Fld25201
                Else -(МощностиДоставки._Fld25201)
            End
        ) > #Temp_TimeService.ВремяВыполнения	
OPTION (OPTIMIZE FOR (@P_DateTimePeriodBegin='{1}',@P_DateTimePeriodEnd='{2}'),KEEP PLAN, KEEPFIXED PLAN);

/*Тут начинаются интервалы, которые рассчитанные*/
Select Distinct
	Case 
		When DATEPART(MINUTE,ГрафикПланирования._Fld23333) > 0 
		Then DATEADD(HOUR,1,ГрафикПланирования._Fld23333) 
		Else ГрафикПланирования._Fld23333 
	End As ВремяВыезда,
	ГрафикПланирования._Fld23321 As Дата,
	ГрафикПланирования._Fld23322RRef As ГруппаПланирования
Into #Temp_CourierDepartureDates
From 
	dbo._InfoRg23320 As ГрафикПланирования With (NOLOCK)
	Inner Join #Temp_PlanningGroups T2 With (NOLOCK) 
	On (ГрафикПланирования._Fld23322RRef = T2.ГруппаПланирования) 
Where ГрафикПланирования._Fld23321 Between @P_DateTimePeriodBegin And @P_DateTimePeriodEnd
	And ГрафикПланирования._Fld23333 > @P_DateTimeNow
OPTION (OPTIMIZE FOR (@P_DateTimePeriodBegin='{1}',@P_DateTimePeriodEnd='{2}',@P_DateTimeNow='{0}'),KEEP PLAN, KEEPFIXED PLAN);

Select
    T5._Period As Период,
    T5._Fld25112RRef As ГруппаПланирования, 
	T5._Fld25111RRef As Геозона,
	T2.Основная As Приоритет,
	T5._Fld25202 As ВремяНачалаНачальное,
	T5._Fld25203 As ВремяОкончанияНачальное,
    DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, T5._Fld25202) As NUMERIC(12)
        ),
        T5._Period
    ) As ВремяНачала,
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, T5._Fld25203) As NUMERIC(12)
        ),
        T5._Period
    ) As ВремяОкончания,
	SUM(
                Case
                    When (T5._RecordKind = 0.0) Then T5._Fld30385
                    Else -(T5._Fld30385)
                End
            ) As КоличествоЗаказовЗаИнтервалВремениВВашеВремя,
	SUM(
                Case
                    When (T5._RecordKind = 0.0) Then T5._Fld25113
                    Else -(T5._Fld25113)
                End
            ) As КоличествоЗаказовЗаИнтервалВремени
Into #Temp_IntervalsAll_old
From
    dbo._AccumRg25110 T5 With (NOLOCK)
	Inner Join #Temp_PlanningGroups T2 With (NOLOCK) 
	On (T5._Fld25112RRef = T2.ГруппаПланирования)
	And T2.Склад In (Select СкладНазначения From #Temp_DateAvailable)
Where
    T5._Period >= @P_DateTimePeriodBegin --begin +2
    And T5._Period <= @P_DateTimePeriodEnd --End
    And T5._Fld25111RRef In (Select Геозона From #Temp_GeoData) 
	And T5._Period In (Select Дата From #Temp_DeliveryPower)
Group By
    T5._Period,
    T5._Fld25112RRef,
    T5._Fld25111RRef,
    T5._Fld25202,
	T5._Fld25203,
	T2.Основная
Having
    (
        CAST(
            SUM(
                Case
                    When (T5._RecordKind = 0.0) Then T5._Fld25113
                    Else -(T5._Fld25113)
                End
            ) As NUMERIC(16, 0)
        ) > 0.0
		And Case When @P_YourTimeDelivery = 1
			Then CAST(
				SUM(
					Case
						When (T5._RecordKind = 0.0) Then T5._Fld30385
						Else -(T5._Fld30385)
					End
				) As NUMERIC(16, 0)
			) 
			Else 1.0
			End > 0.0
    )
OPTION (HASH GROUP, OPTIMIZE FOR (@P_DateTimePeriodBegin='{1}',@P_DateTimePeriodEnd='{2}'), KEEP PLAN, KEEPFIXED PLAN);
;

Select Distinct
	ВременныеИнтервалы.Период As Период,
	ВременныеИнтервалы.Геозона As Геозона,
	ВременныеИнтервалы.ГруппаПланирования As ГруппаПланирования,
	ВременныеИнтервалы.ВремяНачалаНачальное As ВремяНачалаНачальное,
	ВременныеИнтервалы.ВремяОкончанияНачальное As ВремяОкончанияНачальное,
	ВременныеИнтервалы.КоличествоЗаказовЗаИнтервалВремени As КоличествоЗаказовЗаИнтервалВремени,
	ВременныеИнтервалы.КоличествоЗаказовЗаИнтервалВремениВВашеВремя As КоличествоЗаказовЗаИнтервалВремениВВашеВремя,
	ВременныеИнтервалы.ВремяНачала As ВремяНачала,
	ВременныеИнтервалы.ВремяОкончания As ВремяОкончания,
	ВременныеИнтервалы.Приоритет
Into #Temp_IntervalsAll
From
	#Temp_IntervalsAll_old As ВременныеИнтервалы
		Inner Join #Temp_CourierDepartureDates As ВТ_ГрафикПланирования
		On DATEPART(HOUR, ВТ_ГрафикПланирования.ВремяВыезда) <= DATEPART(HOUR, ВременныеИнтервалы.ВремяНачалаНачальное)
		And ВременныеИнтервалы.ГруппаПланирования = ВТ_ГрафикПланирования.ГруппаПланирования
	    And ВременныеИнтервалы.Период = ВТ_ГрафикПланирования.Дата
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld25128) As NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) As ВремяНачала,
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld25129) As NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) As ВремяОкончания,
	Sum(#Temp_IntervalsAll.КоличествоЗаказовЗаИнтервалВремени) As КоличествоЗаказовЗаИнтервалВремени, 
    Case When ГеоЗонаВременныеИнтервалы._Fld27342 = 0x01 Then 1 Else 0 End As Стимулировать,
	#Temp_IntervalsAll.Период,
	#Temp_IntervalsAll.ГруппаПланирования,
	#Temp_IntervalsAll.Геозона,
	#Temp_IntervalsAll.Приоритет
Into #Temp_Intervals
From #Temp_IntervalsAll
	Inner Join _Reference114_VT25126 ГеоЗонаВременныеИнтервалы With (NOLOCK)
		On #Temp_IntervalsAll.Геозона = ГеоЗонаВременныеИнтервалы._Reference114_IDRRef
		And #Temp_IntervalsAll.ВремяНачалаНачальное >= ГеоЗонаВременныеИнтервалы._Fld25128
		And #Temp_IntervalsAll.ВремяНачалаНачальное < ГеоЗонаВременныеИнтервалы._Fld25129
		And @P_YourTimeDelivery = 0
	Inner Join dbo._Reference23294 T2 With (NOLOCK) 
		On (#Temp_IntervalsAll.ГруппаПланирования = T2._IDRRef)
		And (ГеоЗонаВременныеИнтервалы._Fld25128 >= T2._Fld25137)
		And (NOT (((@P_TimeNow >= T2._Fld25138))))
Where
    #Temp_IntervalsAll.Период = @P_DateTimePeriodBegin

Group By 
	ГеоЗонаВременныеИнтервалы._Fld25128,
	ГеоЗонаВременныеИнтервалы._Fld25129,
	#Temp_IntervalsAll.Период,
	#Temp_IntervalsAll.ГруппаПланирования,
	#Temp_IntervalsAll.Геозона,
	#Temp_IntervalsAll.Приоритет,
    ГеоЗонаВременныеИнтервалы._Fld27342
OPTION (OPTIMIZE FOR (@P_DateTimePeriodBegin='{1}'), KEEP PLAN, KEEPFIXED PLAN);

Insert Into #Temp_Intervals
Select
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld25128) As NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) As ВремяНачала,
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld25129) As NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) As ВремяОкончания,
	Sum(#Temp_IntervalsAll.КоличествоЗаказовЗаИнтервалВремени) As КоличествоЗаказовЗаИнтервалВремени,
	Case When ГеоЗонаВременныеИнтервалы._Fld27342 = 0x01 Then 1 Else 0 End As Стимулировать,
	#Temp_IntervalsAll.Период,
	#Temp_IntervalsAll.ГруппаПланирования,
	#Temp_IntervalsAll.Геозона,
	#Temp_IntervalsAll.Приоритет
From #Temp_IntervalsAll
	Inner Join _Reference114_VT25126 ГеоЗонаВременныеИнтервалы With (NOLOCK)
		On #Temp_IntervalsAll.Геозона = ГеоЗонаВременныеИнтервалы._Reference114_IDRRef
		And #Temp_IntervalsAll.ВремяНачалаНачальное >= ГеоЗонаВременныеИнтервалы._Fld25128
		And #Temp_IntervalsAll.ВремяНачалаНачальное < ГеоЗонаВременныеИнтервалы._Fld25129
		And @P_YourTimeDelivery = 0
	Inner Join dbo._Reference23294 T4 With (NOLOCK) 
	On (#Temp_IntervalsAll.ГруппаПланирования = T4._IDRRef)
	And (
		(@P_TimeNow < T4._Fld25140)
		Or (ГеоЗонаВременныеИнтервалы._Fld25128 >= T4._Fld25139)
	)
Where
    #Temp_IntervalsAll.Период = DATEADD(DAY, 1, @P_DateTimePeriodBegin)

Group By 
	ГеоЗонаВременныеИнтервалы._Fld25128,
	ГеоЗонаВременныеИнтервалы._Fld25129,
	#Temp_IntervalsAll.Период,
	#Temp_IntervalsAll.ГруппаПланирования,
	#Temp_IntervalsAll.Геозона,
	#Temp_IntervalsAll.Приоритет,
    ГеоЗонаВременныеИнтервалы._Fld27342
OPTION (OPTIMIZE FOR (@P_DateTimePeriodBegin='{1}'), KEEP PLAN, KEEPFIXED PLAN); 

Insert Into #Temp_Intervals
Select
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld25128) As NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) As ВремяНачала,
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld25129) As NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) As ВремяОкончания,
	Sum(#Temp_IntervalsAll.КоличествоЗаказовЗаИнтервалВремени) As КоличествоЗаказовЗаИнтервалВремени,
	Case When ГеоЗонаВременныеИнтервалы._Fld27342 = 0x01 Then 1 Else 0 End As Стимулировать,
    #Temp_IntervalsAll.Период,
    #Temp_IntervalsAll.ГруппаПланирования,
    #Temp_IntervalsAll.Геозона,
    #Temp_IntervalsAll.Приоритет
From #Temp_IntervalsAll
	Inner Join _Reference114_VT25126 ГеоЗонаВременныеИнтервалы With (NOLOCK)
		On #Temp_IntervalsAll.Геозона = ГеоЗонаВременныеИнтервалы._Reference114_IDRRef
		And #Temp_IntervalsAll.ВремяНачалаНачальное >= ГеоЗонаВременныеИнтервалы._Fld25128
		And #Temp_IntervalsAll.ВремяНачалаНачальное < ГеоЗонаВременныеИнтервалы._Fld25129
		And @P_YourTimeDelivery = 0
Where
	#Temp_IntervalsAll.Период BETWEEN DATEADD(DAY, 2, @P_DateTimePeriodBegin) 
	And @P_DateTimePeriodEnd --begin +2

Group By 
	ГеоЗонаВременныеИнтервалы._Fld25128,
	ГеоЗонаВременныеИнтервалы._Fld25129,
	#Temp_IntervalsAll.Период,
	#Temp_IntervalsAll.ГруппаПланирования,
	#Temp_IntervalsAll.Геозона,
	#Temp_IntervalsAll.Приоритет,
    ГеоЗонаВременныеИнтервалы._Fld27342
OPTION (OPTIMIZE FOR (@P_DateTimePeriodBegin='{1}',@P_DateTimePeriodEnd='{2}'), KEEP PLAN, KEEPFIXED PLAN);

Insert Into #Temp_Intervals
Select
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld30390) As NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) As ВремяНачала,
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld30391) As NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) As ВремяОкончания,
	Sum(#Temp_IntervalsAll.КоличествоЗаказовЗаИнтервалВремени) As КоличествоЗаказовЗаИнтервалВремени,
	0 As Стимулировать,
    #Temp_IntervalsAll.Период,
    #Temp_IntervalsAll.ГруппаПланирования,
    #Temp_IntervalsAll.Геозона,
    #Temp_IntervalsAll.Приоритет
From #Temp_IntervalsAll
	Inner Join _Reference114_VT30388 ГеоЗонаВременныеИнтервалы With (NOLOCK)
		On #Temp_IntervalsAll.Геозона = ГеоЗонаВременныеИнтервалы._Reference114_IDRRef
		And (
			(#Temp_IntervalsAll.ВремяНачалаНачальное >= ГеоЗонаВременныеИнтервалы._Fld30390 And #Temp_IntervalsAll.ВремяНачалаНачальное < ГеоЗонаВременныеИнтервалы._Fld30391)
			Or (#Temp_IntervalsAll.ВремяОкончанияНачальное > ГеоЗонаВременныеИнтервалы._Fld30390 And #Temp_IntervalsAll.ВремяОкончанияНачальное <= ГеоЗонаВременныеИнтервалы._Fld30391)
			)
		And @P_YourTimeDelivery = 1

Where
	#Temp_IntervalsAll.Период Between DATEADD(DAY, 2, @P_DateTimePeriodBegin) And @P_DateTimePeriodEnd --begin +2

Group By 
	ГеоЗонаВременныеИнтервалы._Fld30390,
	ГеоЗонаВременныеИнтервалы._Fld30391,
	#Temp_IntervalsAll.Период,
	#Temp_IntervalsAll.ГруппаПланирования,
	#Temp_IntervalsAll.Геозона,
	#Temp_IntervalsAll.Приоритет
OPTION (OPTIMIZE FOR (@P_DateTimePeriodBegin='{1}',@P_DateTimePeriodEnd='{2}'), KEEP PLAN, KEEPFIXED PLAN);

Insert Into #Temp_Intervals
Select
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld30390) As NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) As ВремяНачала,
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld30391) As NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) As ВремяОкончания,
	Sum(#Temp_IntervalsAll.КоличествоЗаказовЗаИнтервалВремени) As КоличествоЗаказовЗаИнтервалВремени,
	0 As Стимулировать,
	#Temp_IntervalsAll.Период,
	#Temp_IntervalsAll.ГруппаПланирования,
	#Temp_IntervalsAll.Геозона,
	#Temp_IntervalsAll.Приоритет
From #Temp_IntervalsAll
	Inner Join _Reference114_VT30388 ГеоЗонаВременныеИнтервалы With (NOLOCK)
		On #Temp_IntervalsAll.Геозона = ГеоЗонаВременныеИнтервалы._Reference114_IDRRef
		And (
			(#Temp_IntervalsAll.ВремяНачалаНачальное >= ГеоЗонаВременныеИнтервалы._Fld30390 And #Temp_IntervalsAll.ВремяНачалаНачальное < ГеоЗонаВременныеИнтервалы._Fld30391)
			Or (#Temp_IntervalsAll.ВремяОкончанияНачальное > ГеоЗонаВременныеИнтервалы._Fld30390 And #Temp_IntervalsAll.ВремяОкончанияНачальное <= ГеоЗонаВременныеИнтервалы._Fld30391)
			)
		And @P_YourTimeDelivery = 1
	Inner Join dbo._Reference23294 T4 With (NOLOCK) 
	On (#Temp_IntervalsAll.ГруппаПланирования = T4._IDRRef)
    And (
        (@P_TimeNow < T4._Fld25140)
        Or (ГеоЗонаВременныеИнтервалы._Fld30390 >= T4._Fld25139)
    )
Where
    #Temp_IntervalsAll.Период = DATEADD(DAY, 1, @P_DateTimePeriodBegin)

Group By 
	ГеоЗонаВременныеИнтервалы._Fld30390,
	ГеоЗонаВременныеИнтервалы._Fld30391,
	#Temp_IntervalsAll.Период,
	#Temp_IntervalsAll.ГруппаПланирования,
	#Temp_IntervalsAll.Геозона,
	#Temp_IntervalsAll.Приоритет
OPTION (OPTIMIZE FOR (@P_DateTimePeriodBegin='{1}'), KEEP PLAN, KEEPFIXED PLAN);  

Insert Into #Temp_Intervals
Select
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld30390) As NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) As ВремяНачала,
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld30391) As NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) As ВремяОкончания,
	Sum(#Temp_IntervalsAll.КоличествоЗаказовЗаИнтервалВремени) As КоличествоЗаказовЗаИнтервалВремени, 
    0 As Стимулировать,
	#Temp_IntervalsAll.Период,
	#Temp_IntervalsAll.ГруппаПланирования,
	#Temp_IntervalsAll.Геозона,
	#Temp_IntervalsAll.Приоритет
From #Temp_IntervalsAll
	Inner Join _Reference114_VT30388 ГеоЗонаВременныеИнтервалы With (NOLOCK)
		On #Temp_IntervalsAll.Геозона = ГеоЗонаВременныеИнтервалы._Reference114_IDRRef
		And (
			(#Temp_IntervalsAll.ВремяНачалаНачальное >= ГеоЗонаВременныеИнтервалы._Fld30390 And #Temp_IntervalsAll.ВремяНачалаНачальное < ГеоЗонаВременныеИнтервалы._Fld30391)
			Or (#Temp_IntervalsAll.ВремяОкончанияНачальное > ГеоЗонаВременныеИнтервалы._Fld30390 And #Temp_IntervalsAll.ВремяОкончанияНачальное <= ГеоЗонаВременныеИнтервалы._Fld30391)
			)
		And @P_YourTimeDelivery = 1
	Inner Join dbo._Reference23294 T2 With (NOLOCK) 
		On (#Temp_IntervalsAll.ГруппаПланирования = T2._IDRRef)
		And (ГеоЗонаВременныеИнтервалы._Fld30390 >= T2._Fld30397)
		And (NOT (((@P_TimeNow >= T2._Fld25138))))
Where
    #Temp_IntervalsAll.Период = @P_DateTimePeriodBegin

Group By 
	ГеоЗонаВременныеИнтервалы._Fld30390,
	ГеоЗонаВременныеИнтервалы._Fld30391,
	#Temp_IntervalsAll.Период,
	#Temp_IntervalsAll.ГруппаПланирования,
	#Temp_IntervalsAll.Геозона,
	#Temp_IntervalsAll.Приоритет
OPTION (OPTIMIZE FOR (@P_DateTimePeriodBegin='{1}'), KEEP PLAN, KEEPFIXED PLAN); 

Select 
	Период, 
	Max(Приоритет) As Приоритет 
Into #Temp_PlanningGroupPriority 
From #Temp_Intervals 
Group By Период;
/*Выше закончились рассчитанные интервалы*/

WITH T(date) As (
    /*Это получение списка дат интервалов после даты окончания расчета*/
    Select
        Case When @P_DateTimePeriodEnd >= CAST(CAST(#Temp_DateAvailable.DateAvailable  As DATE) As DATETIME) 
			Then DateAdd(day, 1, @P_DateTimePeriodEnd)
		Else 
			CAST(CAST(#Temp_DateAvailable.DateAvailable  As DATE) As DATETIME) 
		End
	From #Temp_DateAvailable
    
	Union All

    Select
        DateAdd(day, 1, T.date)
    From
        T
		Inner Join #Temp_DateAvailable 
		On T.date < DateAdd(DAY, @P_DaysToShow, CAST(CAST(#Temp_DateAvailable.DateAvailable  As DATE) As DATETIME)) 
)
/*Тут мы выбираем даты из регистра*/
Select 
	#Temp_Intervals.ВремяНачала As ВремяНачала,
	#Temp_Intervals.ВремяОкончания As ВремяОкончания,
	SUM(#Temp_Intervals.КоличествоЗаказовЗаИнтервалВремени) AS КоличествоЗаказовЗаИнтервалВремени
Into #Temp_IntervalsWithOutShifting
From
#Temp_Intervals With (NOLOCK)
Inner Join #Temp_DateAvailable With (NOLOCK) 
    On #Temp_Intervals.ВремяНачала >= #Temp_DateAvailable.DateAvailable
	Inner Join #Temp_TimeService With (NOLOCK) 
	On 1=1
	Inner Join #Temp_PlanningGroupPriority With (NOLOCK) 
	On #Temp_Intervals.Период = #Temp_PlanningGroupPriority.Период 
	And #Temp_Intervals.Приоритет = #Temp_PlanningGroupPriority.Приоритет
Where #Temp_Intervals.Период >= @P_DateTimePeriodBegin
Group By 
	#Temp_Intervals.ВремяНачала,
	#Temp_Intervals.ВремяОкончания,
	#Temp_Intervals.Период,
	#Temp_TimeService.ВремяВыполнения,
    #Temp_Intervals.Стимулировать
Having SUM(#Temp_Intervals.КоличествоЗаказовЗаИнтервалВремени) > #Temp_TimeService.ВремяВыполнения

Union All
/*А тут мы выбираем даты где логисты еще не рассчитали*/
Select
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld25128) As NUMERIC(12)
        ),
        date
    ) As ВремяНачала,
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld25129) As NUMERIC(12)
        ),
        date
    ) As ВремяОкончания,
	0 As КоличествоЗаказовЗаИнтервалВремени
From
    T 
	Inner Join _Reference114_VT25126 As ГеоЗонаВременныеИнтервалы  With (NOLOCK) 
	On ГеоЗонаВременныеИнтервалы._Reference114_IDRRef In (Select Геозона From #Temp_GeoData)
    And @P_IsDelivery = 1
	And @P_YourTimeDelivery = 0
	Inner Join #Temp_DateAvailable On DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld25128) As NUMERIC(12)
        ),
        date
    ) >= #Temp_DateAvailable.DateAvailable

Union All

Select 
	#Temp_AvailablePickUp.ВремяНачала,
	#Temp_AvailablePickUp.ВремяОкончания,
	0
From #Temp_AvailablePickUp
OPTION (OPTIMIZE FOR (@P_DateTimePeriodBegin='{1}',@P_DateTimePeriodEnd='{2}'), KEEP PLAN, KEEPFIXED PLAN);

Select 
	IntervalsWithOutShifting.ВремяНачала
Into #Temp_UnavailableDates
From #Temp_Goods As TempGoods
	Inner Join dbo._InfoRg28348 As ПрослеживаемыеТоварныеКатегории WITH(NOLOCK)
	On 1 = @P_ApplyShifting -- это будет значение ГП ПрименятьСмещениеДоступностиПрослеживаемыхМаркируемыхТоваров
	And ПрослеживаемыеТоварныеКатегории._Fld28349RRef = TempGoods.ТоварнаяКатегорияСсылка
	And @P_DateTimeNow <= DateAdd(DAY, @P_DaysToShift, ПрослеживаемыеТоварныеКатегории._Period)  -- количество дней будет из ГП
	Inner Join #Temp_IntervalsWithOutShifting As IntervalsWithOutShifting
	On IntervalsWithOutShifting.ВремяНачала between ПрослеживаемыеТоварныеКатегории._period 
	And DateAdd(DAY, @P_DaysToShift, ПрослеживаемыеТоварныеКатегории._Period)
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select Top 1
	1
From #Temp_IntervalsWithOutShifting As IntervalsWithOutShifting  
	Left Join #Temp_UnavailableDates As UnavailableDates 
	On IntervalsWithOutShifting.ВремяНачала = UnavailableDates.ВремяНачала
Where 
	UnavailableDates.ВремяНачала is NULL
OPTION (KEEP PLAN, KEEPFIXED PLAN);";
    }
}

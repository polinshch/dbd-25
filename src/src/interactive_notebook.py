import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _(pd):
    agents_df = pd.read_excel("data/agents_reworked.xlsx")

    agents_df
    return (agents_df,)


@app.cell
def _():
    import pandas as pd
    return (pd,)


@app.cell
def _(agents_df, pd):
    from db import get_engine

    agents_df['start_date'] = pd.to_datetime(agents_df['start_date'], dayfirst=True)
    agents_df['end_date'] = pd.to_datetime(agents_df['end_date'], dayfirst=True)
    agents_df['type'] = agents_df['type'].fillna('')

    agents_df_to_db = agents_df[['fullname', 'id', 'type', 'start_date', 'end_date']]
    agents_df_to_db.columns = ['Name', 'NumberFromMinyst', 'Type', 'StartDate', 'EndDate']
    agents_df_to_db.to_sql(name="Agents", con=get_engine(), if_exists="append", index=False)
    return (get_engine,)


@app.cell
def _(get_engine):
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    from sqlalchemy import text

    plt.rcParams['figure.figsize'] = (12, 6)
    plt.rcParams['figure.dpi'] = 100
    engine = get_engine()
    return engine, plt


@app.cell
def _(engine, pd):
    query_top_months = """
    SELECT 
        TO_CHAR(DATE_TRUNC('month', "StartDate"), 'YYYY-MM') as "Месяц",
        COUNT(*) as "Количество",
        STRING_AGG(DISTINCT "Type", '; ') as "Типы"
    FROM "Agents"
    WHERE "StartDate" IS NOT NULL
    GROUP BY DATE_TRUNC('month', "StartDate")
    ORDER BY "Количество" DESC
    LIMIT 10;
    """

    df_top_months = pd.read_sql(query_top_months, engine)
    df_top_months
    return


@app.cell
def _(engine, pd):
    query_by_month = """
    SELECT 
        DATE_TRUNC('month', "StartDate") as month,
        COUNT(DISTINCT "Id") as count,
        "Type"
    FROM "Agents"
    WHERE "StartDate" IS NOT NULL
    GROUP BY DATE_TRUNC('month', "StartDate"), "Type"
    ORDER BY month;
    """
    df_monthly = pd.read_sql(query_by_month, engine)
    df_monthly['month'] = pd.to_datetime(df_monthly['month'])

    pivot_table = df_monthly.pivot_table(
        index='month', 
        columns='Type', 
        values='count', 
        fill_value=0,
        aggfunc='sum'
    )

    pivot_table['ИТОГО'] = pivot_table.sum(axis=1)
    pivot_table['Накопленный итог'] = pivot_table['ИТОГО'].cumsum()

    type_cols = [col for col in pivot_table.columns if col not in ['ИТОГО', 'Накопленный итог']]
    pivot_table = pivot_table[['ИТОГО', 'Накопленный итог'] + type_cols]

    pivot_table
    return (pivot_table,)


@app.cell
def _(pivot_table, plt):
    fig, ax = plt.subplots(figsize=(14, 6))

    pivot_for_plot = pivot_table.drop(columns=['ИТОГО', 'Накопленный итог'])
    pivot_for_plot.plot(kind='bar', stacked=True, ax=ax, width=0.8, colormap='tab10')

    ax.set_title('Количество включённых в реестр по месяцам', fontsize=12, fontweight='bold')
    ax.set_xlabel('Месяц')
    ax.set_ylabel('Количество')
    ax.legend(title='Тип', bbox_to_anchor=(1.02, 1), loc='upper left')

    tick_labels = [d.strftime('%Y-%m') for d in pivot_for_plot.index]
    step = max(1, len(tick_labels) // 15)
    ax.set_xticks(range(0, len(tick_labels), step))
    ax.set_xticklabels([tick_labels[i] for i in range(0, len(tick_labels), step)], rotation=45, ha='right')
    ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    plt.show()
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(engine, mo):
    _ = mo.sql(
        f"""
        INSERT INTO "Laws" ("Type", "Title", "StartDate") VALUES ('КоАП РФ', 'Дубликат', '2024-01-01');
        """,
        engine=engine
    )
    return


@app.cell
def _(engine, mo):
    _df = mo.sql(
        f"""
        INSERT INTO "Courts" ("Name", "RegionId") VALUES ('Несуществующий суд', '99');
        """,
        engine=engine
    )
    return


@app.cell
def _(engine, mo):
    _df = mo.sql(
        f"""
        INSERT INTO "Laws" ("Type", "Title", "StartDate") VALUES ('ТК РФ', 'Трудовой Кодекс Российской Федерации', '2002-02-01');
        """,
        engine=engine
    )
    return


@app.cell
def _(engine, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM "Laws";
        """,
        engine=engine
    )
    return


if __name__ == "__main__":
    app.run()

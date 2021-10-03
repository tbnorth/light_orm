"""Same as demo.py, but use object.
"""
from light_orm.light_orm import LightOrm

DB_SQL = [
    """create table topping (
        topping integer primary key,
        name text
    )""",
    """create table pizza (
        pizza integer primary key,
        name text
    )""",
    """create table ingredient (
        ingredient integer primary key,
        pizza int,
        topping int,
        grams int
    )""",
]


def main():

    pizzas = {
        "margarita": ["cheese", "tomato", "basil"],
        "hawaiian": ["cheese", "pineapple", "ham"],
        "vegetarian": ["cheese", "tomato", "pepper", "mushroom"],
    }

    lo = LightOrm("pizza.db", DB_SQL)

    for pizza_name, toppings in pizzas.items():
        pizza, new = lo.get_or_make_pk("pizza", {"name": pizza_name})
        for topping_name in toppings:
            topping, new = lo.get_or_make_pk("topping", {"name": topping_name})
            lo.get_or_make_rec("ingredient", dict(pizza=pizza, topping=topping))
    lo.con.commit()

    print("%d pizzas" % lo.do_one("select count(*) as n from pizza").n)
    for pizza in lo.do_query(
        "select name, count(*) as n from pizza "
        "join ingredient using (pizza) group by name"
    ):
        print("%s %d ingredients" % (pizza.name, pizza.n))

    # we get this far without needing the ingredient pk in the ingredient table
    # but if we want to edit the ingredient records, the pk is needed:

    # set topping grams to length of pizza name, naturally
    for pizza in lo.get_recs("pizza"):
        for ingredient in lo.get_recs("ingredient", {"pizza": pizza.pizza}):
            ingredient.grams = len(pizza.name)
            lo.save_rec(ingredient)
    lo.con.commit()

    # re-open DB just because
    lo = LightOrm("pizza.db", DB_SQL)

    for pizza in lo.get_recs("pizza"):
        print("\n%s\n%s" % (pizza.name, "=" * len(pizza.name)))
        for ingredient in lo.get_recs("ingredient", {"pizza": pizza.pizza}):
            topping = lo.get_rec("topping", {"topping": ingredient.topping})
            print("  %s, %dg" % (topping.name, ingredient.grams))
        # OR
        for ingredient in lo.do_query(
            "select topping.name, grams from pizza "
            "join ingredient using (pizza) join topping using (topping) "
            "where pizza=?",
            [pizza.pizza],
        ):
            # print("  %s, %dg" % (ingredient.name, ingredient.grams))
            pass


if __name__ == "__main__":
    main()

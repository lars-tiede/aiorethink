import asyncio

import pytest
import rethinkdb as r

import aiorethink as ar


@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_heroes(aiorethink_db_session, event_loop):
    cn = await ar.db_conn

    class Publisher(ar.Document):
        name = ar.Field(
                ar.StringValueType(),
                indexed = True)


    class Studio(ar.Document):
        name = ar.Field(
                ar.StringValueType(),
                indexed = True)


    class Hero(ar.Document):
        name = ar.Field(
                ar.StringValueType(),
                indexed = True)
        publisher = ar.Field(
                ar.LazyDocRefValueType(Publisher)
                )

        async def get_movies(self):
            return await Movie.from_query(
                    Movie.cq().filter(r.row["heroes"].contains(self.pkey))
                    )

        async def get_movies_as_list(self):
            iterator = await self.get_movies()
            return await iterator.as_list()


    HeroRef = ar.LazyDocRefValueType(Hero)

    class Movie(ar.Document):
        name = ar.Field(
                ar.StringValueType(),
                indexed = True)
        year = ar.Field(
                ar.IntValueType(),
                indexed = True)
        studio = ar.Field(
                ar.LazyDocRefValueType(Studio))
        heroes = ar.Field(
                ar.ListValueType(HeroRef)
                )
        #heroes = ar.Field()



    async def track_table_changes(doc_cls, num_changes):
        i = 0
        names = []
        async for doc, change_msg in await doc_cls.aiter_table_changes():
            i += 1
            assert isinstance(doc, ar.Document)
            assert "new_val" in change_msg
            names.append(doc.name)
            if i >= num_changes:
                break
        return names


    async def track_doc_changes(doc, num_changes):
        i = 0
        changed_keys = []
        async for doc, keys, change_msg in await doc.aiter_changes():
            i += 1
            assert isinstance(doc, ar.Document)
            assert "new_val" in change_msg
            changed_keys.append(keys)
            if i >= num_changes:
                break
        return changed_keys


    async def track_heroes_named_man(num_changes):
        i = 0
        names = []
        query = Hero.cq().\
                filter(r.row["name"].match("man$")).\
                changes()
        async for hero, message in await ar.aiter_changes(query, ar.AnyValueType()):
            i += 1
            assert isinstance(hero["name"], str)
            names.append(hero["name"])
            if i >= num_changes:
                break
        return names



    async def fill_our_db():
        marvel = await Publisher.create(name = "Marvel")
        dc = await Publisher.create(name = "DC")

        marvel_studios = await Studio.create(name = "Marvel Studios")
        warner = await Studio.create(name = "Warner Bros. Pictures")
        fox = await Studio.create(name = "20th Century Fox")

        spiderman = await Hero.create(name = "Spiderman", publisher = ar.lval(marvel))
        deadpool = await Hero.create(name = "Deadpool", publisher = ar.lazy_doc_ref(marvel))
        superman = await Hero.create(name = "Superman", publisher = ar.lazy_doc_ref(dc))
        batman = await Hero.create(name = "Batman", publisher = ar.lazy_doc_ref(dc))

        movies = [
                {"name": "Batman vs Superman", "year": 2016, "studio": ar.lazy_doc_ref(warner),
                    "heroes": [ar.lazy_doc_ref(superman), ar.lazy_doc_ref(batman)]},
                {"name": "Deadpool", "year": 2016, "studio": ar.lazy_doc_ref(fox), "heroes": [ar.lazy_doc_ref(deadpool)]},
                ]
        for movie in movies:
            await Movie.create(**movie)


    await ar.init_app_db()

    hero_tracker = event_loop.create_task(track_table_changes(Hero, 3))
    hero_named_man_tracker = event_loop.create_task(track_heroes_named_man(2))
    await fill_our_db()
    done, pending = await asyncio.wait([hero_named_man_tracker, hero_tracker], timeout=1.0)
    assert hero_tracker in done
    assert hero_tracker.exception() == None
    assert hero_tracker.result() == ["Spiderman", "Deadpool", "Superman"]
    assert hero_named_man_tracker.exception() == None
    assert hero_named_man_tracker in done
    assert hero_named_man_tracker.result() == ["Spiderman", "Superman"]


    spiderman = await Hero.from_query(
            Hero.cq().get_all("Spiderman", index="name").nth(0)
            )

    assert "id" in spiderman.keys()
    assert "name" in spiderman.keys()
    assert "publisher" in spiderman.keys()
    assert len(spiderman.keys()) == 3
    assert len(spiderman.keys(ar.UNDECLARED_ONLY)) == 0
    for k in spiderman.keys():
        assert spiderman.get(k, "boohoo") != "boohoo"
    assert "Spiderman" in spiderman.values()
    assert len(spiderman.values()) == 3
    for v in spiderman.values():
        assert v != "boohoo"
    assert ("name", "Spiderman") in spiderman.items()
    assert len(spiderman.items()) == 3

    spiderman2 = await Hero.load(spiderman.pkey) # load into another object
    spidey_tracker = event_loop.create_task(track_doc_changes(spiderman, 2))
    spiderman2.name = "Spidahman"
    await spiderman2.save()
    spiderman2.name = "Spiderman"
    await spiderman2.save()
    done, pending = await asyncio.wait([spidey_tracker], timeout=1.0)
    assert spidey_tracker in done
    assert spidey_tracker.exception() == None
    assert spidey_tracker.result() == [["name"], ["name"]]

    sp_movies = await spiderman.get_movies_as_list()
    assert sp_movies == []

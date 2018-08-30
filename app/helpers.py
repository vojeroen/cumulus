from app.errors import ObjectDoesNotExist, MultipleObjectsFound


def one(l):
    if len(l) == 0:
        raise ObjectDoesNotExist()
    elif len(l) > 1:
        raise MultipleObjectsFound()
    return l[0]

from django.db import models


class TestModelA(models.Model):
    a = models.CharField(max_length=200)
    b = models.IntegerField()
    c = models.IntegerField()

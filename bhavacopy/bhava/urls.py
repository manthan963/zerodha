from django.urls import path
from . import views
urlpatterns = [
    path('', views.extract_copy_zip, name='download copy'),

]

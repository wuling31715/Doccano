import pandas as pd
import csv
import json
from io import TextIOWrapper
import itertools as it
import logging

from django.contrib.auth.views import LoginView as BaseLoginView
from django.urls import reverse
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import get_object_or_404
from django.views import View
from django.views.generic import TemplateView, CreateView
from django.views.generic.list import ListView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib import messages
from django.contrib.auth.models import User
from django.contrib.auth import authenticate


from .permissions import SuperUserMixin
from .forms import ProjectForm
from mixer.backend.django import mixer
from .models import Document, Project, Label, Annotation, SequenceAnnotation
from app import settings

from django.db import connection


logger = logging.getLogger(__name__)



class IndexView(TemplateView):
    template_name = 'index.html'


class ProjectView(LoginRequiredMixin, TemplateView):

    def get_template_names(self):
        project = get_object_or_404(Project, pk=self.kwargs['project_id'])
        return [project.get_template_name()]


class ProjectsView(LoginRequiredMixin, CreateView):
    form_class = ProjectForm
    template_name = 'projects.html'


class DatasetView(SuperUserMixin, LoginRequiredMixin, ListView):
    template_name = 'admin/dataset.html'
    paginate_by = 100

    def get_queryset(self):
        project = get_object_or_404(Project, pk=self.kwargs['project_id'])
        return project.documents.all()

class LabelView(SuperUserMixin, LoginRequiredMixin, TemplateView):
    template_name = 'admin/label.html'


class StatsView(SuperUserMixin, LoginRequiredMixin, TemplateView):
    template_name = 'admin/stats.html'


class GuidelineView(SuperUserMixin, LoginRequiredMixin, TemplateView):
    template_name = 'admin/guideline.html'


class DataUpload(SuperUserMixin, LoginRequiredMixin, TemplateView):
    template_name = 'admin/dataset_upload.html'

    class ImportFileError(Exception):
        def __init__(self, message):
            self.message = message

    def extract_metadata_csv(self, row, text_col, header_without_text):
        vals_without_text = [val for i, val in enumerate(row) if i != text_col]
        return json.dumps(dict(zip(header_without_text, vals_without_text)))

    def csv_to_documents(self, project, file, text_key='text'):
        form_data = TextIOWrapper(file, encoding='utf-8')
        reader = csv.reader(form_data)

        maybe_header = next(reader)
        if maybe_header:
            if text_key in maybe_header:
                text_col = maybe_header.index(text_key)
            elif len(maybe_header) == 1:
                reader = it.chain([maybe_header], reader)
                text_col = 0
            else:
                raise DataUpload.ImportFileError("CSV file must have either a title with \"text\" column or have only one column ")

            header_without_text = [title for i, title in enumerate(maybe_header)
                                   if i != text_col]

            return (
                Document(
                    text=row[text_col],
                    metadata=self.extract_metadata_csv(row, text_col, header_without_text),
                    project=project
                )
                for row in reader
            )
        else:
            return []

    def extract_metadata_json(self, entry, text_key):
        copy = entry.copy()
        del copy[text_key]
        try: 
            # return copy["entities"]
            return json.dumps(copy)
        except:
            return {}

    def json_to_documents(self, project, file, text_key='text'):
        parsed_entries = (json.loads(line) for line in file)
        return (
            Document(text=entry[text_key], metadata=self.extract_metadata_json(entry, text_key), project=project)
            for entry in parsed_entries
        )
    
    def insert_document(self, entry, project_id):
        with connection.cursor() as cursor:
            text = entry['text']
            metadata = self.extract_metadata_json(entry, 'text')
            command = """insert into server_document ('text', 'project_id', 'metadata') values (\'{}\', {}, \'{}\');""".format(text, project_id, metadata)
            cursor.execute(command)

    def label_text_to_id(self, project_id):
        with connection.cursor() as cursor:
            command = 'select * from server_label where project_id = {};'.format(project_id)
            cursor.execute(command)
            server_label = cursor.fetchall()
            label_dict = dict()
            for label in server_label:
                label_dict[label[1]] = label[0]
        return label_dict

    def insert_annotation(self, entry, label_dict, project_id, user_id):
        entities = entry["entities"]
        with connection.cursor() as cursor:
            command = 'select * from server_document where project_id = {};'.format(project_id)
            cursor.execute(command)
            server_document = cursor.fetchall()
            document_id = server_document[-1][0]
            if type(entities) == str:
                entities = eval(entities)
            for annotation in entities:
                command = """insert into server_sequenceannotation ("prob", "manual", "start_offset", "end_offset", "document_id", "label_id", "user_id") values ({}, {}, {}, {}, {}, {}, {});""".format(0.0, 0, annotation[0], annotation[1], document_id, label_dict[annotation[2]], user_id)
                cursor.execute(command)

    def get_file_format(self, file_name):
        file_format = file_name.split(".")
        return file_format[-1]

    def txt_to_dict(self, txt):
        text = str()
        for line in txt:
            line = line.decode('utf-8')
            text += line
        dictt = dict()
        dictt['text'] = text
        return dictt
        
    def file_to_dict(self, file, file_format):
        if file_format == 'xlsx':
            data_frame = pd.read_excel(file)
        elif file_format == 'csv':
            data_frame = pd.read_csv(file)
        dict_list = list()
        try:
            for text, entities in zip(data_frame['text'], data_frame['entities']):
                dictt = dict()
                dictt['text'] = text
                dictt['entities'] = entities
                dict_list.append(dictt)
        except:
            for text in data_frame['text']:
                dictt = dict()
                dictt['text'] = text
                dict_list.append(dictt)
        return dict_list

    def post(self, request, *args, **kwargs):
        user_id = request.user.id
        project = get_object_or_404(Project, pk=kwargs.get('project_id'))
        project_id = kwargs.get('project_id')
        # import_format = request.POST['format']    
        label_dict = self.label_text_to_id(project_id)    
        try:
            file_format = self.get_file_format(request.FILES['file'].name)
            file = request.FILES['file'].file            
            if file_format == 'txt':
                entry = self.txt_to_dict(file)
                self.insert_document(entry, project_id)
            else:
                if file_format == 'json':            
                    parsed_entries = (json.loads(line) for line in file)
                else:
                    parsed_entries = self.file_to_dict(file, file_format)
                for entry in parsed_entries:
                    self.insert_document(entry, project_id)
                    try:
                        self.insert_annotation(entry, label_dict, project_id, user_id)
                    except:
                        pass                
            
            return HttpResponseRedirect(reverse('dataset', args=[project.id]))
        except DataUpload.ImportFileError as e:
            messages.add_message(request, messages.ERROR, e.message)
            return HttpResponseRedirect(reverse('upload', args=[project.id]))
        except Exception as e:
            logger.exception(e)
            messages.add_message(request, messages.ERROR, e)
            return HttpResponseRedirect(reverse('upload', args=[project.id]))

def delete(request, *args, **kwargs):
    # project = get_object_or_404(Project, pk=kwargs.get('project_id'))
    project_id = kwargs.get('project_id')
    document_id = kwargs.get('document_id')
    with connection.cursor() as cursor:
        command = """delete from server_document where id = {};""".format(document_id)
        cursor.execute(command)
    return HttpResponseRedirect(reverse('dataset', args=[project_id]))

class DataDownload(SuperUserMixin, LoginRequiredMixin, TemplateView):
    template_name = 'admin/dataset_download.html'


class DataDownloadFile(SuperUserMixin, LoginRequiredMixin, View):

    def get(self, request, *args, **kwargs):
        project_id = self.kwargs['project_id']
        project = get_object_or_404(Project, pk=project_id)
        docs = project.get_documents(is_null=False).distinct()
        export_format = request.GET.get('format')
        filename = '_'.join(project.name.lower().split())
        try:
            if export_format == 'csv':
                response = self.get_csv(filename, docs)
            elif export_format == 'json':
                response = self.get_json(filename, docs)
            return response
        except Exception as e:
            logger.exception(e)
            messages.add_message(request, messages.ERROR, e)
            return HttpResponseRedirect(reverse('download', args=[project.id]))

    def get_csv(self, filename, docs):
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="{}.csv"'.format(filename)
        writer = csv.writer(response)
        for d in docs:
            writer.writerows(d.to_csv())
        return response

    def get_json(self, filename, docs):
        response = HttpResponse(content_type='text/json')
        response['Content-Disposition'] = 'attachment; filename="{}.json"'.format(filename)
        for d in docs:
            dump = json.dumps(d.to_json(), ensure_ascii=False)
            response.write(dump + '\n')  # write each json object end with a newline
        return response


class LoginView(BaseLoginView):
    template_name = 'login.html'
    redirect_authenticated_user = True
    extra_context = {
        'github_login': bool(settings.SOCIAL_AUTH_GITHUB_KEY),
        'aad_login': bool(settings.SOCIAL_AUTH_AZUREAD_TENANT_OAUTH2_TENANT_ID),
    }

    def get_context_data(self, **kwargs):
        context = super(LoginView, self).get_context_data(**kwargs)
        context['social_login_enabled'] = any(value for key, value in context.items()
                                              if key.endswith('_login'))
        return context


class DemoTextClassification(TemplateView):
    template_name = 'demo/demo_text_classification.html'


class DemoNamedEntityRecognition(TemplateView):
    template_name = 'demo/demo_named_entity.html'


class DemoTranslation(TemplateView):
    template_name = 'demo/demo_translation.html'

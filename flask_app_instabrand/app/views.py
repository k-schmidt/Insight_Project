from flask import render_template, url_for

import app
from app import helper_methods


@app.app.route('/<username>')
def user_photos(username):
    timeline = list(helper_methods.get_user_timeline(username, app.session))

    templateData = {
            'size' : "big",  # request.args.get('size','thumb'),
            'media' : timeline  # recent_media
    }

    return render_template('base.html', **templateData)


# Redirect users to Instagram for login
@app.app.route('/connect')
def main():
    pass  #return redirect(url)


# # Instagram will redirect users back to this route after successfully logging in
# @app.route('/instagram_callback')
# def instagram_callback():

#         code = request.args.get('code')

#         if code:

#                 access_token, user = api.exchange_code_for_access_token(code)
#                 if not access_token:
#                         return 'Could not get access token'

#                 app.logger.debug('got an access token')
#                 app.logger.debug(access_token)

#                 # Sessions are used to keep this data
#                 session['instagram_access_token'] = access_token
#                 session['instagram_user'] = user

#                 return redirect('/') # redirect back to main page

#         else:
#                 return "Uhoh no code provided"



@app.app.errorhandler(404)
def page_not_found(error):
    return render_template('404.html'), 404


# This is a jinja custom filter
@app.app.template_filter('strftime')
def _jinja2_filter_datetime(date, fmt=None):
    pyDate = time.strptime(date,'%a %b %d %H:%M:%S +0000 %Y') # convert instagram date string into python date/time
    return time.strftime('%Y-%m-%d %h:%M:%S', pyDate) # return the formatted date.

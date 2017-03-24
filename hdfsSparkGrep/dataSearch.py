import web
from web import form
import subprocess
import json
from subprocess import call, Popen, STDOUT, PIPE
from datetime import datetime, timedelta, date


render = web.template.render('templates/', globals={'json':json})
urls = (
    '/', 'dataSearch'
)

SCRIPT_BASE = '/Users/smadhavan/HandsOn/git/hdfsSparkGrep'
def runProcess(exe):    
    p = subprocess.call(exe, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
 
email = ''
sdt = ''
edt = ''
skey = ''

curdt = datetime.today() - timedelta(days=2)
strtdt = datetime.today() - timedelta(days=8)
defEndDt = datetime.strftime(curdt, '%Y-%m-%d')
defStDt = datetime.strftime(strtdt, '%Y-%m-%d')

register_form = form.Form(
    form.Textbox("email", form.notnull, value='xyz@mydomain.com', description="User E-Mail"),
    form.Textbox("sdt", form.notnull, value=defStDt, description="Search start-date (yyyy-mm-dd)", maxlength="10"),
    form.Textbox("edt", form.notnull, value=defEndDt, description="Search end-date (yyyy-mm-dd)", maxlength="10"),
    form.Textbox("skey", form.notnull, description="Search key (any keyword from raw json", maxlength="24"),
    form.Button("submit", type="submit", description="Register"),
)

class dataSearch:
    def GET(self):
        # do $:f.render() in the template
        f = register_form()
        return render.searchForm(f)

    def POST(self):
        f = register_form()
        if not f.validates():
            return render.searchForm(f)
        #else:
	# do whatever is required for registration
	#i = web.input(email=None, skey=None, sdt=None, edt=None)
	email = f['email'].value
	sdt = f['sdt'].value
	edt = f['edt'].value
	skey = f['skey'].value
	if not email or not skey or not sdt:
	    return render.errMsg(dumps(f))

	user = email.split('@')[0] 
	runProcess(SCRIPT_BASE + '/callSparkSearch.sh '+user+ ' '+skey+ ' '+sdt+ ' '+edt+ ' ')
	data = ''
	with open('/tmp/searchresult-'+user+'.txt', 'r') as myfile: 
	    data=myfile.read()
	return render.index(email, skey, sdt, edt, data)

if __name__ == "__main__":
    app = web.application(urls, globals())
    app.internalerror = web.debugerror
    app.run()


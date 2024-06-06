# -*- coding: utf-8 -*-
"""

@author: Nikhil & Urjit
"""


from flask import Flask, request, render_template, url_for
from time import time
from search_class import searchFunctions

app = Flask(__name__, static_folder="static")

search = searchFunctions()


@app.route("/")
def home():
    return render_template("/html/form.html")


@app.route("/search", methods=["GET", "POST"])
def searchOptions():
    try:
        if request.method == "POST":
            option = request.form["options"]
            query = request.form["query"]
        if option == "keyword":
            start_time = time()
            data = search.get_keyword(query)
            end_time = time()
            print(f"The time taken for the execution of the query: {end_time - start_time} second(s)")
        elif option == "hashtag":
            start_time = time()
            data = search.get_hashtag(query)
            end_time = time()
            print(f"The time taken for the execution of the query: {end_time - start_time} second(s)")
        else:
            start_time = time()
            data = search.get_username(query)
            end_time = time()
            print(f"The time taken for the execution of the query: {end_time - start_time} second(s)")  
                
        return render_template("/html/query.html", name=query, value=data.to_html())
    except Exception as e:
        return render_template("/html/missing.html", name=query)

@app.route("/top_10_tweets")
def top_10_tweets():
    start_time = time()
    data = search.get_top_10_tweets()
    end_time = time()
    print(f"The time taken for the execution of the query: {end_time - start_time} second(s)")
    return render_template("/html/top_10.html", value=data.to_html())


@app.route("/top_10_users")
def top_10_users():
    start_time = time()
    data = search.get_top_10_users()
    end_time = time()
    print(f"The time taken for the execution of the query: {end_time - start_time} second(s)")
    return render_template("/html/top_10.html", value=data.to_html())


if __name__ == "__main__":
    app.run(debug=True)
    app.add_url_rule('/favicon.ico',
                 redirect_to=url_for('static', filename='favicon.ico'))
    app.add_url_rule('/style.css',
                 redirect_to=url_for('static', filename='style.css'))

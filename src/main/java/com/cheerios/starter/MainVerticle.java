package com.cheerios.starter;


import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rjeschke.txtmark.Processor;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.templ.freemarker.FreeMarkerTemplateEngine;

public class MainVerticle extends AbstractVerticle {

	private static final String SQL_CREATE_PAGES_TABLE = "create table if not exists Pages (Id integer identity primary key, Name varchar(255) unique, Content clob)";
	private static final String SQL_GET_PAGE = "select Id, Content from Pages where Name = ?"; 
	private static final String SQL_CREATE_PAGE = "insert into Pages values (NULL, ?, ?)";
	private static final String SQL_SAVE_PAGE = "update Pages set Content = ? where Id = ?";
	private static final String SQL_ALL_PAGES = "select Name from Pages";
	private static final String SQL_DELETE_PAGE = "delete from Pages where Id = ?";

	private JDBCClient jdbcClient;

	private static final Logger logger = LoggerFactory.getLogger(MainVerticle.class);

	private FreeMarkerTemplateEngine freeMakerTemplate;

	private static final String EMPTY_PAGE_MARKDOWN = "# A new page\n\nFeel-free to write in Markdown!\n";

	@Override
	public void start(Promise<Void> promise) throws Exception {
		Future<Void> step = prepareDatabase().compose(v -> startHttpServer());
		step.setHandler(ar -> {
			if(ar.succeeded()) {
				promise.complete();
			}else {
				promise.fail(ar.cause());
			}
		});
	}

	private Future<Void> prepareDatabase(){
		Promise<Void> promise = Promise.promise();

		jdbcClient = JDBCClient.createShared(vertx, new JsonObject()
				.put("url", "jdbc:hsqldb:file:db/wiki") 
				.put("driver_class", "org.hsqldb.jdbcDriver") 
				.put("max_pool_size", 30));

		jdbcClient.getConnection(ar -> {
			if(ar.failed()) {
				logger.error("Failed to obtain connection from database", ar.cause());
				promise.fail(ar.cause());
			}else {
				SQLConnection connection = ar.result();
				connection.execute(SQL_CREATE_PAGES_TABLE, create -> {
					connection.close();
					if(create.failed()) {
						logger.debug("Could not create tables", create.cause());
						promise.fail(create.cause());
					}else {
						logger.debug("Tables created");
						promise.complete();
					}
				});
			}
		});

		return promise.future();

	}

	private Future<Void> startHttpServer(){
		Promise<Void> promise = Promise.promise();

		HttpServer server = vertx.createHttpServer();
		Router router = Router.router(vertx);
		router.get("/").handler(this::indexHandler);
		router.get("/wiki/:page").handler(this::pageRenderingHandler);
		router.post().handler(BodyHandler.create());
		router.post("/save").handler(this::pageUpdateHandler);
		router.post("/create").handler(this::pageCreateHandler);
		router.post("/delete").handler(this::pageDeletionHandler);

		freeMakerTemplate = FreeMarkerTemplateEngine.create(vertx);

		server.requestHandler(router).listen(8080, ar -> {
			if(ar.succeeded()) {
				logger.info("Http server running on 8080");
				promise.complete();
			}else {
				logger.info("Http server failed to start", ar.cause());
				promise.fail("failed to start");
			}
		});


		return promise.future();
	}

	private void indexHandler(RoutingContext context) {
		jdbcClient.getConnection(ar -> {
			if(ar.succeeded()) {
				SQLConnection connection = ar.result();
				connection.query(SQL_ALL_PAGES, res -> {
					connection.close();
					if(res.succeeded()) {
						List<String> pages = res.result().getResults().stream().map(json -> json.getString(0)).sorted().collect(Collectors.toList());
						context.put("title", "wiki home");
						context.put("pages", pages);
						freeMakerTemplate.render(context.data(), "templates/index.ftl", tr -> {
							if(tr.succeeded()) {
								context.response().putHeader("Content-Type", "text/html");
								context.response().end(tr.result());
							}else {
								logger.error("freeMakerTemplate Connection failed " + ar.cause());

								context.fail(tr.cause());
							}
						});
					}else {
						logger.error("Sql Connection failed ", res.cause());
						context.fail(res.cause());
					}
				});
			}else {
				logger.error("jdbc Connection failed ", ar.cause());

				context.fail(ar.cause());
			}
		});
	}



	private void pageRenderingHandler(RoutingContext context) {
		String page = context.request().getParam("page");
		jdbcClient.getConnection(ar -> {
			if(ar.succeeded()) {
				SQLConnection connection = ar.result();
				connection.queryWithParams(SQL_GET_PAGE, new JsonArray().add(page), res -> {
					if(res.succeeded()) {
						JsonArray row = res.result().getResults().stream().findFirst().orElse(new JsonArray().add(-1).add(EMPTY_PAGE_MARKDOWN));
						Integer id = row.getInteger(0);
						String rawContent = row.getString(1);

						context.put("title", page);
						context.put("id", id);
						context.put("newPage", res.result().getResults().isEmpty() ? "yes" : "no");
						context.put("rawContent", rawContent);
						context.put("content", Processor.process(rawContent)); 
						context.put("timestamp", new Date().toString());

						freeMakerTemplate.render(context.data(), "templates/page.ftl", tr -> {
							if(tr.succeeded()) {
								context.response().putHeader("Content-Type", "text/html");
								context.response().end(tr.result());
							}else {
								context.fail(tr.cause());
							}
						});
					}else {
						context.fail(res.cause());
					}
				});
			}else {
				context.fail(ar.cause());
			}
		});
	}

	private void pageCreateHandler(RoutingContext context) {
		String name = context.request().getParam("name");
		String location = "/wiki".concat(StringUtils.isNotBlank(name) ? "/name": "/");

		context.response().putHeader("location", location);
		context.response().setStatusCode(303);
		context.response().end();
	}

	private void pageUpdateHandler(RoutingContext context) {
		String id = context.request().getParam("id"); 
		String title = context.request().getParam("title");
		String markdown = context.request().getParam("markdown");
		boolean newPage = "yes".equals(context.request().getParam("newPage"));

		jdbcClient.getConnection(ar -> {
			if(ar.succeeded()) {
				SQLConnection connection = ar.result();
				String sql = newPage ? SQL_CREATE_PAGE : SQL_SAVE_PAGE;
				JsonArray array = new JsonArray();

				if(newPage) {
					array.add(title).add(markdown);
				}else {
					array.add(markdown).add(id);
				}

				connection.updateWithParams(sql, array, res ->{
					if(res.succeeded()) {
						context.response().putHeader("location", "/wiki/"+title);
						context.response().setStatusCode(303);
						context.response().end();
					}else {
						context.fail(res.cause());
					}
				});
			}else {
				context.fail(ar.cause());
			}
		});
	}


	private void pageDeletionHandler(RoutingContext context) {
		String id = context.request().getParam("id");

		jdbcClient.getConnection(ar -> {
			if(ar.succeeded()) {
				SQLConnection connection = ar.result();
				connection.updateWithParams(SQL_DELETE_PAGE, new JsonArray().add(id), res -> {
					if(res.succeeded()) {
						context.response().putHeader("location", "/");
						context.response().setStatusCode(303);
						context.response().end();
					}else {
						context.fail(res.cause());
					}
				});
			}else {
				context.fail(ar.cause());
			}

		});
	}

}

package com.bol.test.assignment.aggregator;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bol.test.assignment.offer.Offer;
import com.bol.test.assignment.offer.OfferCondition;
import com.bol.test.assignment.offer.OfferService;
import com.bol.test.assignment.order.Order;
import com.bol.test.assignment.order.OrderService;
import com.bol.test.assignment.product.Product;
import com.bol.test.assignment.product.ProductService;

public class AggregatorService {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	private final OrderService orderService;
	private final OfferService offerService;
	private final ProductService productService;

	public AggregatorService(OrderService orderService, OfferService offerService, ProductService productService) {
		this.orderService = orderService;
		this.offerService = offerService;
		this.productService = productService;
	}

	/**
	 * Enrich.
	 *
	 * @param sellerId the seller id
	 * @return the enriched order
	 * @throws ExecutionException the execution exception
	 * @throws InterruptedException the interrupted exception
	 */
	public EnrichedOrder enrich(int sellerId) throws ExecutionException, InterruptedException {
		Order order = getOrderWithSellerId(sellerId);
		Offer offer = null;
		Product product = null;
		/* In order to ensure a high performance, we need to execute the offer and product services in parallel*/
		/* create a task for offer service */
		Callable<Object> offerTask = new Callable<Object>() {
			public Object call() throws Exception {
				LOGGER.debug("get offer by offer id task started");
				return getOfferByOfferId(order);
			}
		};
		/* create a task for product service */
		Callable<Object> productTask = new Callable<Object>() {
			public Object call() throws Exception {
				LOGGER.debug("get product by product id task started");
				return getProductByProductId(order);
			}
		};
		ExecutorService executor = Executors.newFixedThreadPool(2);
		List<Future<Object>> futuresList = executor.invokeAll(Arrays.asList(offerTask, productTask));
		/* retrieve results */
		for(Future<Object> future : futuresList) {
			if (future.get() instanceof Offer){
				offer = (Offer)future.get();
			}else if (future.get() instanceof Product){
				product = (Product)future.get();
			}
		}
		/* we need to shut down the executor */
		executor.shutdown();
		LOGGER.debug("executor shut down");

		return combine(order, offer, product) ;
	}

	/**
	 * Combine.
	 *
	 * @param order the order
	 * @param offer the offer
	 * @param product the product
	 * @return the enriched order
	 */
	private EnrichedOrder combine(Order order, Offer offer, Product product) {
		if (offer == null && product == null){
			return new EnrichedOrder(order.getId(), -1, OfferCondition.UNKNOWN, -1, null);
		}
		else if (offer == null){
			return new EnrichedOrder(order.getId(), -1, OfferCondition.UNKNOWN, product.getId(), product.getTitle());
		}
		else if (product == null){
			return new EnrichedOrder(order.getId(), offer.getId(), offer.getCondition(), -1, null);
		}
		return new EnrichedOrder(order.getId(), offer.getId(), offer.getCondition(), product.getId(), product.getTitle());
	}

	/**
	 * Gets the order with seller id.
	 *
	 * @param sellerId the seller id
	 * @return the order with seller id
	 * @throws RuntimeException the runtime exception
	 */
	private Order getOrderWithSellerId(int sellerId) throws RuntimeException {
		try {
			Order order = orderService.getOrder(sellerId);
			LOGGER.debug("order with seller id " + sellerId + " retrieved successfully");
			return order;
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			throw new RuntimeException("Order service failed", e);
		}
	}

	/**
	 * Gets the offer by offer id.
	 *
	 * @param order the order
	 * @return the offer by offer id
	 */
	private Offer getOfferByOfferId(Order order) {
		Offer offer = null;
		try {
			offer = offerService.getOffer(order.getOfferId());
			LOGGER.debug("Offer with id " + order.getOfferId() + " retrieved successfully");
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		return offer;
	}

	/**
	 * Gets the product by product id.
	 *
	 * @param order the order
	 * @return the product by product id
	 */
	private Product getProductByProductId(Order order) {
		Product product = null;
		try {
			product = productService.getProduct(order.getProductId());
			LOGGER.debug("Product with id " + order.getProductId() + " retrieved successfully");
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		return product;
	}
}
